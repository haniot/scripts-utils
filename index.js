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

eventbus.logger('info')

eventbus
    .createConnection(rabbitmq_uri, {retries: 1, interval: 2000})
    .then((conn) => {
        connection = conn
        console.log('======== (0/ Eventbus connected \\0) ========')
    })
    .catch(err => console.log('Eventbus error', err.message))


// User Data Info

const user_id = '5e991d071e68e30019863931' // THE HANIOT USER ID
const start_date = '2019-11-10' // THE THE START DATE
const end_date = '2019-11-12' // THE INTERVAL END DATE

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

        try {
            // Sleep Sync
            const sleep_before = '2019-11-12'
            const sleep_limit = 100
            const sleep = await getSleepData(token, sleep_before, sleep_limit)

            // Sleep Parse
            const new_sleep = sleep.map(item => parseSleep(item, user_id))

            // Publish Sleep
            if (new_sleep.length && new_sleep.length > 0) {
                await pubEventBus('activities', 'sleep.sync', {
                    event_name: 'SleepSyncEvent',
                    timestamp: new Date(),
                    type: 'activities',
                    sleep: new_sleep
                }, options)
            }

            // Weigh Sync
            const weight_base_date = '2019-11-01'
            const weight_end_date = '2019-11-12'
            const weights = await getWeightData(token, weight_base_date, weight_end_date)

            // Weight Parse
            const new_weight = weights.map(weight => parseWeight(weight, user_id))

            // Publish Weight
            if (new_weight.length && new_weight.length > 0) {
                await pubEventBus('measurements', 'weights.sync', {
                    event_name: 'WeightSyncEvent',
                    timestamp: new Date(),
                    type: 'measurements',
                    weight: new_weight
                }, options)
            }

            // Activities Sync
            const activities_before = '2019-11-12'
            const activities_limit = 100
            const activities = await getActivityData(token, activities_before, activities_limit)

            // Activities Parse
            const new_activities = activities.map(activity => parseActivity(activity, user_id))

            // Publish Activity
            if (new_activities.length && new_activities.length > 0) {
                await pubEventBus('activities', 'physicalactivities.sync', {
                    event_name: 'PhysicalActivitySyncEvent',
                    timestamp: new Date(),
                    type: 'activities',
                    physical_activity: new_activities
                }, options)
            }

            for await (let date of dates) {
                // Intraday Timeseries Sync
                const steps = await getIntradayTimeSeries(token, 'steps', date)
                const distance = await getIntradayTimeSeries(token, 'distance', date)
                const calories = await getIntradayTimeSeries(token, 'calories', date)
                const minutes_fairly_active = await getIntradayTimeSeries(token, 'minutesFairlyActive', date)
                const minutes_very_active = await getIntradayTimeSeries(token, 'minutesVeryActive', date)
                const minutes_active = mergeIntradayTimeSeriesValues(minutes_fairly_active, minutes_very_active)
                const heart_rate = await getHeartRateIntradayTimeSeries(token, date, '1sec')

                // Intraday Timeseries Parse
                const steps_intraday = parseIntradayTimeSeriesResources(user_id, 'steps', steps)
                const distance_intraday = parseIntradayTimeSeriesResources(user_id, 'distance', distance)
                const calories_intraday = parseIntradayTimeSeriesResources(user_id, 'calories', calories)
                const minutes_active_intraday = parseIntradayTimeSeriesResources(user_id, 'active_minutes', minutes_active)
                const heart_rate_intraday = parseIntradayTimeSeriesHeartRate(user_id, heart_rate)

                // Publish Data
                await pubEventBus(exchange, routing_key, buildMessage(steps_intraday), options)
                await pubEventBus(exchange, routing_key, buildMessage(distance_intraday), options)
                await pubEventBus(exchange, routing_key, buildMessage(calories_intraday), options)
                await pubEventBus(exchange, routing_key, buildMessage(minutes_active_intraday), options)
                await pubEventBus(exchange, routing_key, buildMessage(heart_rate_intraday), options)
            }
        } catch (err) {
            console.log('error at sync data', e)
            return res.send('An error occurs during the sync process, but the token is available on file token.txt ' +
                'in your project directory.')
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

function getWeightData(token, baseDate, endDate) {
    const path = `/body/log/weight/date/${baseDate}/${endDate}.json`
    return new Promise((resolve, reject) => {
        getDataFromPath(path, token)
            .then(result => resolve(result.weight))
            .catch(err => reject(fitbitClientErrorListener(err, token)))
    })
}

function getSleepData(token, beforeDate, limit) {
    const path = `/sleep/list.json?beforeDate=${beforeDate}&sort=desc&offset=0&limit=${limit}`
    return new Promise((resolve, reject) => {
        getDataFromPath(path, token)
            .then(result => resolve(result.sleep))
            .catch(err => reject(fitbitClientErrorListener(err, token)))
    })
}

function getActivityData(token, beforeDate, limit) {
    const path = `/activities/list.json?beforeDate=${beforeDate}&sort=desc&offset=0&limit=${limit}`
    return new Promise((resolve, reject) => {
        getDataFromPath(path, token)
            .then(result => resolve(result.activities))
            .catch(err => reject(fitbitClientErrorListener(err, token)))
    })
}

function parseIntradayTimeSeriesResources(userId, resource, dataset) {
    const intraday_data = dataset[`activities-${resource}-intraday`]
    const dataset_intraday = intraday_data.dataset
    const date = dataset[`activities-${resource}`][0].dateTime
    return {
        patient_id: userId,
        start_time: dataset_intraday.length ?
            moment(date).format('YYYY-MM-DDT').concat(dataset_intraday[0].time) : undefined,
        end_time: dataset_intraday.length ?
            moment(date).format('YYYY-MM-DDT').concat(dataset_intraday[dataset_intraday.length - 1].time) : undefined,
        interval: `${intraday_data.datasetInterval}${intraday_data.datasetType.substr(0, 3)}`,
        type: resource,
        data_set: dataset_intraday
    }
}

function parseIntradayTimeSeriesHeartRate(userId, data) {
    const heart_rate_zone = data['activities-heart'][0].value.heartRateZones
    const intraday_data = data['activities-heart-intraday']
    const dataset_intraday = intraday_data.dataset
    const date = data['activities-heart'][0].dateTime

    const fat_burn = heart_rate_zone.filter(value => value.name === 'Fat Burn')[0]
    const cardio = heart_rate_zone.filter(value => value.name === 'Cardio')[0]
    const peak = heart_rate_zone.filter(value => value.name === 'Peak')[0]
    const out_of_range = heart_rate_zone.filter(value => value.name === 'Out of Range')[0]

    return {
        patient_id: userId,
        start_time: dataset_intraday.length ?
            moment(date).format('YYYY-MM-DDT').concat(dataset_intraday[0].time) : undefined,
        end_time: dataset_intraday.length ?
            moment(date).format('YYYY-MM-DDT').concat(dataset_intraday[dataset_intraday.length - 1].time) : undefined,
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

function parseWeight(json, patient_id) {
    const result = {patient_id}
    if (json.date !== undefined && json.time !== undefined) {
        result.timestamp = moment(json.date.concat('T').concat(json.time)).utc().format()
    }
    if (json.weight !== undefined) result.value = json.weight
    if (json.fat !== undefined) result.body_fat = json.fat
    result.unit = 'kg'
    return result
}

function parseSleep(json, patient_id) {
    const result = {patient_id}
    if (json.startTime !== undefined) {
        result.start_time = moment(json.startTime).utc().format()
        if (json.duration !== undefined) {
            result.end_time = moment(json.startTime).add(json.duration, 'milliseconds').utc().format()
            result.duration = json.duration
        }
    }
    if (json.type !== undefined) result.type = json.type
    if (json.levels && json.levels.data && json.levels.data.length) {
        result.pattern = {
            data_set: json.levels.data.map(value => {
                return {
                    start_time: moment(value.dateTime).utc().format(),
                    name: value.level === 'wake' ? 'awake' : value.level,
                    duration: `${parseInt(value.seconds, 10) * 1000}`
                }
            }),
            summary: getSleepSummary(json.levels.summary)
        }
    }
    return result
}

function parseActivity(json, patient_id) {
    const result = {patient_id}
    if (json.startTime !== undefined) {
        result.start_time = moment(json.startTime).utc().format()
        result.end_time = moment(json.startTime).add(json.duration, 'milliseconds').utc().format()
    }
    if (json.duration !== undefined) result.duration = json.duration
    if (json.activityName !== undefined) result.name = json.activityName
    if (json.calories !== undefined) result.calories = json.calories
    if (json.caloriesLink !== undefined) result.calories_link = json.caloriesLink
    if (json.heartRateLink !== undefined) result.heart_rate_link = json.heartRateLink
    if (json.steps !== undefined) result.steps = json.steps
    if (json.distance !== undefined && json.distanceUnit !== undefined) {
        result.distance = convertDistanceToMetter(json.distance, json.distanceUnit)
    }
    if (json.activityLevel !== undefined && json.activityLevel.length) {
        result.levels = json.activityLevel.map(level => {
            return {duration: level.minutes * 60000, name: level.name}
        })
    }
    if (json.averageHeartRate !== undefined) {
        result.heart_rate_average = json.averageHeartRate
        if (json.heartRateZones !== undefined) {
            const peak = json.heartRateZones.find(zone => zone.name === 'Peak')
            const out_of_range = json.heartRateZones.find(zone => zone.name === 'Out of Range')
            const cardio = json.heartRateZones.find(zone => zone.name === 'Cardio')
            const fat_burn = json.heartRateZones.find(zone => zone.name === 'Fat Burn')
            peak.duration = peak.minutes * 60000
            out_of_range.duration = out_of_range.minutes * 60000
            cardio.duration = cardio.minutes * 60000
            fat_burn.duration = fat_burn.minutes * 60000
            result.heart_rate_zones = {peak, out_of_range, cardio, fat_burn}
        }
    }
    return result
}

function convertDistanceToMetter(distance, unit) {
    return unit === 'Kilometer' ? distance * 1000 : distance * 1609.344
}

function getSleepSummary(summary) {
    if (summary.asleep && summary.awake && summary.restless) {
        return {
            asleep: {count: summary.asleep.count, duration: summary.asleep.minutes * 60000},
            awake: {count: summary.awake.count, duration: summary.awake.minutes * 60000},
            restless: {count: summary.restless.count, duration: summary.restless.minutes * 60000}
        }
    }
    return {
        deep: {count: summary.deep.count, duration: summary.deep.minutes * 60000},
        light: {count: summary.light.count, duration: summary.light.minutes * 60000},
        rem: {count: summary.rem.count, duration: summary.rem.minutes * 60000},
        awake: {count: summary.wake.count, duration: summary.wake.minutes * 60000}
    }
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

function isSameMonth(dateOne, dateTwo) {
    return moment(dateOne).isSame(dateTwo, 'month');
}
