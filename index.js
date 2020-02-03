const bodyParser = require('body-parser')
const express = require('express')
const FitbitApiClient = require('fitbit-node')
const fs = require('fs')
const moment = require('moment')
const eventbus = require('amqp-client-node').amqpClient
const Message = require('amqp-client-node').Message
require('dotenv').config()

// API Config
const PORT_HTTP = process.env.PORT_HTTP
const app = express()
app.use(bodyParser.urlencoded({extended: true}))
app.use(bodyParser.json())
app.listen(PORT_HTTP, () => console.log('Server listening on port', PORT_HTTP))

// Fitbit Client Config
const CALLBACK_URL = process.env.CALLBACK_URL
const client = new FitbitApiClient({
    clientId: process.env.FITBIT_CLIENT_ID,
    clientSecret: process.env.FITBIT_CLIENT_SECRET
})

// AMQP Eventbus Config
const rabbitmq_uri = process.env.RABBITMQ_URI || 'amqp://guest:guest@127.0.0.1:5672'
const options = {
    consumer: {
        noAck: true
    },
    exchange: {
        type: 'topic'
    },
    receiveFromYourself: true
}

let connection
const queue = 'fitbit-api-token'
const exchange = 'timeseries'
const routing_key = 'intraday.sync'

eventbus
    .createConnection(rabbitmq_uri, {retries: 1, interval: 2000})
    .then((conn) => {
        connection = conn
        console.log('======== (0/ Eventbus connected \\0) ========')
    })
    .catch(err => console.log('Eventbus error', err.message))


// User Data Info

const user_id = '5e3851f1b20a6e5813304269'
const start_date = '2020-01-27'
const end_date = '2020-01-31'

// Routes
app.get('/', (req, res) => {
    res.redirect(getCode())
})

app.get('/callback', (req, res) => {
    client.getAccessToken(req.query.code, CALLBACK_URL).then(async result => {
        // use the access token to fetch the user's profile information
        fs.writeFileSync(__dirname + '/token.txt', JSON.stringify(result))

        const token = result.access_token
        const dates = getAllDates(start_date, end_date)
        for await (let date of dates) {
            try {
                const steps = await getIntradayTimeSeries(token, 'steps', date)
                const distance = await getIntradayTimeSeries(token, 'distance', date)
                const calories = await getIntradayTimeSeries(token, 'calories', date)
                const minutes_fairly_active = await getIntradayTimeSeries(token, 'minutesFairlyActive', date)
                const minutes_very_active = await getIntradayTimeSeries(token, 'minutesVeryActive', date)
                const minutes_active = mergeIntradayTimeSeriesValues(minutes_fairly_active, minutes_very_active)
                const heart_rate = await getHeartRateIntradayTimeSeries(token, date, '1sec')

                const steps_intraday = parseIntradayTimeSeriesResources(user_id, 'steps', steps)
                const distance_intraday = parseIntradayTimeSeriesResources(user_id, 'distance', distance)
                const calories_intraday = parseIntradayTimeSeriesResources(user_id, 'calories', calories)
                const minutes_active_intraday = parseIntradayTimeSeriesResources(user_id, 'active_minutes', minutes_active)
                const heart_rate_intraday = parseIntradayTimeSeriesHeartRate(user_id, heart_rate)

                await pubEventBus(exchange, routing_key, buildMessage(steps_intraday), options)
                await pubEventBus(exchange, routing_key, buildMessage(distance_intraday), options)
                await pubEventBus(exchange, routing_key, buildMessage(calories_intraday), options)
                await pubEventBus(exchange, routing_key, buildMessage(minutes_active_intraday), options)
                await pubEventBus(exchange, routing_key, buildMessage(heart_rate_intraday), options)

            } catch (e) {
                console.log('error at sync data', e)
                return res.send('An error occurs during the sync process, but the token is available on file token.txt ' +
                    'in your project directory.')
            }
        }
        return res.send('The token was saved successfully on project directory (file token.txt) and the ' +
            'intraday timeseries was published on eventbus!')
    }).catch(function (error) {
        res.send(error)
    })
})


app.get('/ok', (req, res) => {
    res.send('Token saved successful. Verify on file token.txt on project directory.')
})

// Functions
function getCode() {
    try {
        return client.getAuthorizeUrl('activity heartrate profile sleep weight', CALLBACK_URL)
    } catch (err) {
        console.log('Error at get authorize url', err)
    }
}

function pubEventBus(exchange, routingKey, message, options) {
    connection
        .pub(exchange, routingKey, new Message(message), options)
        .then(() => console.log('published successfully!'))
        .catch(err => console.log('Pub error', err.message))
}

function getDataFromPath(path, accessToken) {
    return new Promise((resolve, reject) => {
        client.get(path, accessToken)
            .then(data => {
                if (data[0].errors) return reject(fitbitClientErrorListener(data[0].errors[0]))
                return resolve(data[0])
            })
            .catch(err => reject(fitbitClientErrorListener(err)))
    })
}

function getIntradayTimeSeries(token, resource, date) {
    const path = `/activities/${resource}/date/${date}/${date}.json`
    return new Promise((resolve, reject) => {
        getDataFromPath(path, token)
            .then(result => resolve(result))
            .catch(err => reject(fitbitClientErrorListener(err)))
    })
}

function getHeartRateIntradayTimeSeries(token, date, detailLevel) {
    const path = `/activities/heart/date/${date}/1d/${detailLevel}.json`
    return new Promise((resolve, reject) => {
        getDataFromPath(path, token)
            .then(result => resolve(result))
            .catch(err => reject(fitbitClientErrorListener(err, token)))
    })
}

function parseIntradayTimeSeriesResources(userId, resource, dataset) {
    const intraday_data = dataset[`activities-${resource}-intraday`]
    const dataset_intraday = intraday_data.dataset
    return {
        patient_id: userId,
        start_time: dataset_intraday.length ?
            moment().format('YYYY-MM-DDT').concat(dataset_intraday[0].time) : undefined,
        end_time: dataset_intraday.length ?
            moment().format('YYYY-MM-DDT').concat(dataset_intraday[dataset_intraday.length - 1].time) : undefined,
        interval: `${intraday_data.datasetInterval}${intraday_data.datasetType.substr(0, 3)}`,
        type: resource,
        data_set: dataset_intraday
    }
}

function parseIntradayTimeSeriesHeartRate(userId, data) {
    const heart_rate_zone = data['activities-heart'][0].value.heartRateZones
    const intraday_data = data['activities-heart-intraday']
    const dataset_intraday = intraday_data.dataset

    const fat_burn = heart_rate_zone.filter(value => value.name === 'Fat Burn')[0]
    const cardio = heart_rate_zone.filter(value => value.name === 'Cardio')[0]
    const peak = heart_rate_zone.filter(value => value.name === 'Peak')[0]
    const out_of_range = heart_rate_zone.filter(value => value.name === 'Out of Range')[0]

    return {
        patient_id: userId,
        start_time: dataset_intraday.length ?
            moment().format('YYYY-MM-DDT').concat(dataset_intraday[0].time) : undefined,
        end_time: dataset_intraday.length ?
            moment().format('YYYY-MM-DDT').concat(dataset_intraday[dataset_intraday.length - 1].time) : undefined,
        type: 'heart_rate',
        interval: `${intraday_data.datasetInterval}${intraday_data.datasetType.substr(0, 3)}`,
        zones: {
            fat_burn: {
                min: fat_burn.min,
                max: fat_burn.max,
                duration: fat_burn.minutes * 60000 || 0,
                calories: fat_burn.caloriesOut || 0
            },
            cardio: {
                min: cardio.min,
                max: cardio.max,
                duration: cardio.minutes * 60000 || 0,
                calories: cardio.caloriesOut || 0
            },
            peak: {
                min: peak.min,
                max: peak.max,
                duration: peak.minutes * 60000 || 0,
                calories: peak.caloriesOut || 0
            },
            out_of_range: {
                min: out_of_range.min,
                max: out_of_range.max,
                duration: out_of_range.minutes * 60000 || 0,
                calories: out_of_range.caloriesOut || 0
            }
        },
        data_set: dataset_intraday
    }
}

function mergeIntradayTimeSeriesValues(intradayOne, intradayTwo) {
    const dataset_one = intradayOne['activities-minutesFairlyActive-intraday'].dataset
    const dataset_two = intradayTwo['activities-minutesVeryActive-intraday'].dataset

    const result = {
        'activities-active_minutes': [{
            dateTime: intradayOne['activities-minutesFairlyActive'][0].dateTime,
            value: parseInt(intradayOne['activities-minutesFairlyActive'][0].value, 10) +
                parseInt(intradayTwo['activities-minutesVeryActive'][0].value, 10)
        }],
        'activities-active_minutes-intraday': {
            dataset: [],
            datasetInterval: intradayOne['activities-minutesFairlyActive-intraday'].datasetInterval,
            datasetType: intradayOne['activities-minutesFairlyActive-intraday'].datasetType
        }
    }

    for (let i = 0; i < dataset_one.length; i++) {
        if (dataset_one[i].time === dataset_two[i].time) {
            result['activities-active_minutes-intraday'].dataset.push({
                time: dataset_one[i].time,
                value: parseInt(dataset_one[i].value, 10) + parseInt(dataset_two[i].value, 10)
            })
        }
    }
    return result
}

function fitbitClientErrorListener(err) {
    if (err.context) return {type: err.context.errors[0].errorType, message: err.context.errors[0].message}
    else if (err.code === 'EAI_AGAIN') return {type: 'client_error', message: err.message}
    return {type: err.errorType, message: err.message}
}

function getAllDates(startDate, endDate) {
    let date_init = startDate
    const days = moment(endDate).diff(startDate, 'day')

    const dates = []
    for (let i = 0; i <= days; i++) {
        dates.push(date_init)
        date_init = moment(date_init).add(1, 'day').format('YYYY-MM-DD')
    }

    return dates
}

function buildMessage(intraday) {
    return {
        event_name: 'IntradayTimeSeriesSyncEvent',
        timestamp: new Date(),
        type: 'timeseries',
        intraday
    }
}
