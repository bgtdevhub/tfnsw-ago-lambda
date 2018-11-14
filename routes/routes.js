const gtfsrb = require('gtfs-realtime-bindings');
const request = require('request');
const fs = require('fs');
const protobuf = require('protobufjs');

// App to retrieve short lived token
const client_id = '';
const client_secret = '';

// ArcGIS Url
const oauth2Url = 'https://www.arcgis.com/sharing/rest/oauth2/token/';
const featureServerUrl = '';
const featureServerUploadUrl = `${featureServerUrl}/uploads/upload`;
const featureServerAppendUrl = `${featureServerUrl}/0/append`;
const featureServerDeleteUrl = `${featureServerUrl}/0/deleteFeatures`;

const congestionLevel = {
    0: "UNKNOWN_CONGESTION_LEVEL",
    1: "RUNNING_SMOOTHLY",
    2: "STOP_AND_GO",
    3: "CONGESTION",
    4: "SEVERE_CONGESTION"
};

const occupancyStatus = {
    0: "EMPTY",
    1: "MANY_SEATS_AVAILABLE",
    2: "FEW_SEATS_AVAILABLE",
    3: "STANDING_ROOM_ONLY",
    4: "CRUSHED_STANDING_ROOM_ONLY",
    5: "FULL",
    6: "NOT_ACCEPTING_PASSENGERS"
}

function getVehiclesPosition(type) {

    return new Promise(function (resolve, reject) {
        let url = '';

        switch (type) {
            case 'buses':
                url = 'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses';
                break;
            
            case 'ferries':
                url = 'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/ferries';
                break;

            case 'lightrail':
                url = 'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/lightrail';
                break;

            case 'nswtrains':
                url = 'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/nswtrains';
                break;

            case 'sydneytrains':
                url = 'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/sydneytrains';
                break;

            default:
                url = 'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/sydneytrains';

        }
        request(
            {
                url: url,
                headers: { 'authorization':'' },
                method: 'GET',
                encoding: null
            }, function (error, res, body) {
                if (!error && res.statusCode == 200) {
                    protobuf.load('gtfs.proto', (err, root) => {
                        const feedMessage = root.lookupType("transit_realtime.FeedMessage");
                        const feed = feedMessage.decode(body);
                        resolve(feed.entity);
                    });
                } else {
                    reject(error);
                }
            }
        );
    });
}

const createCSV = (vehiclesPosition, transitType) => {

    return new Promise(function (resolve, reject) {
        const csvHeader = 'vehicle_id,trip_id,route_id,bearing,start_date,start_time,speed,congestion_level,occupancy_status,TransitType,longitude,latitude,APIdate\n';

        const vehiclesSeen = [];
        let rowsToInsert = '';

        vehiclesPosition.forEach(vehicle => {

            if (vehicle &&
                vehicle.vehicle &&
                vehicle.vehicle.position &&
                vehicle.vehicle.trip) {

                if (transitType === 'sydneytrains' && vehicle.vehicle.vehicle.id.length > 40) {
                    vehicle.vehicle.vehicle.id = "R.29";
                }

                if (transitType === 'buses' && vehicle.vehicle.vehicle['.transit_realtime.tfnswVehicleDescriptor'].performingPriorTrip) {
                    return;
                }

                const vehicleExisted = vehiclesSeen.filter(v => v === vehicle.vehicle.vehicle.id);

                if (!vehicleExisted.length) {
                    
                    let geometry = {
                        longitude: vehicle.vehicle.position.longitude,
                        latitude: vehicle.vehicle.position.latitude
                    }

                    let attributes = {
                        vehicle_id: vehicle.vehicle.vehicle.id || '',
                        trip_id: vehicle.vehicle.trip.tripId || '',
                        route_id: vehicle.vehicle.trip.routeId || '',
                        bearing: vehicle.vehicle.position.bearing || '',
                        start_date: vehicle.vehicle.trip.startDate || '',
                        start_time: vehicle.vehicle.trip.startTime || '',
                        speed: vehicle.vehicle.position.speed || '',
                        congestion_level: congestionLevel[vehicle.vehicle.congestionLevel] || 'UNKNOWN_CONGESTION_LEVEL',
                        occupancy_status: occupancyStatus[vehicle.vehicle.occupancyStatus] || '',
                        TransitType: transitType,
                        APIdate: vehicle.vehicle.timestamp.toString() || ''
                    }

                    rowsToInsert += `${attributes.vehicle_id},${attributes.trip_id},${attributes.route_id},${attributes.bearing},`
                                        + `${attributes.start_date},${attributes.start_time},${attributes.speed},${attributes.congestion_level},`
                                        + `${attributes.occupancy_status},${attributes.TransitType},`
                                        + `${geometry.longitude},${geometry.latitude},${attributes.APIdate}\n`;
                    vehiclesSeen.push(vehicle.vehicle.vehicle.id);
                }
            }
        })

        const data = csvHeader + rowsToInsert;

        fs.writeFile(`/tmp/feed-${transitType}.csv`, data, function(err, data){
            if (err) reject(err);
            resolve(`/tmp/feed-${transitType}.csv`);
        });
    });
}

const requestToken = () => {

    // generate a token with client id and client secret
    return new Promise(function (resolve, reject) {
        request.post({
            url: oauth2Url,
            json: true,
            form: {
                f: 'json',
                client_id: client_id,
                client_secret: client_secret,
                grant_type: 'client_credentials',
                expiration: '1440'
            }
        }, function(error, response, body){
            if (error) reject(error);
            
            resolve(body.access_token);
        });
    });
}

const uploadCSV = (filename, token) => {
    
    return new Promise(function (resolve, reject) {
        request.post({
            url: featureServerUploadUrl,
            json: true,
            formData: {
                csv_file: fs.createReadStream(filename),
                f: 'json',
                token
            }            
        }, function(error, response, body){
            if (error) reject(error);

            if (body.success) {
                resolve(body.item.itemID);
            } else {
                reject("Error in getting upload ID.")
            }
        });
    });
}

const upsert = (appendUploadId, token) => {
    
    return new Promise(function (resolve, reject) {

        const fieldMappings = [
            { "source": "vehicle_id", "name":"vehicle_id" },
            { "source": "trip_id", "name":"trip_id" },
            { "source": "route_id", "name":"route_id" },
            { "source": "bearing", "name":"bearing" },
            { "source": "start_date", "name":"start_date" },
            { "source": "start_time", "name":"start_time" },
            { "source": "speed", "name":"speed" },
            { "source": "congestion_level", "name":"congestion_level" },
            { "source": "occupancy_status", "name":"occupancy_status" },
            { "source": "TransitType", "name":"TransitType" },
            { "source": "longitude", "name":"longitude" },
            { "source": "latitude", "name":"latitude" },
            { "source": "APIdate", "name":"APIdate" }
        ];

        const appendSourceInfo = {
            type: "csv",
            useBulkInserts: true,         
            sourceUrl: "",
            locationType: "coordinates",
            longitudeFieldName: "longitude",
            latitudeFieldName: "latitude",            
            columnDelimiter: ",",
            qualifier: "\"",
            sourceSR: {
                "wkid": 4326,
                "latestWkid": 4326
            }
        };

        const formData = {
            f: 'json',
            fieldMappings: JSON.stringify(fieldMappings),
            appendSourceInfo: JSON.stringify(appendSourceInfo),
            upsert: 'true',
            skipInserts: 'false',
            skipUpdates: 'false',
            useGlobalIds: 'false',
            updateGeometry: 'true',
            upsertMatchingField: 'vehicle_id',
            appendUploadId,
            appendUploadFormat: 'csv',
            rollbackOnFailure: 'false',
            token
        }

        request.post({
            url: featureServerAppendUrl,
            json: true,
            formData,
        }, function(error, response, body){
            if (error) reject(error);
            
            if (body.statusUrl) {
                resolve(body.statusUrl)
            } else {
                reject(body);
            }
        });
    });    
}

const deleteVehicle = (token) => {
    
    return new Promise(function (resolve, reject) {
        const now = new Date();
        const timeForDelete = Math.floor(now.getTime() / 1000) - 60; // every minute

        const whereClause = "APIdate <= " + timeForDelete;
        
        const formData = {
            f: 'json',
            where: whereClause,
            token
        }

        request.post({
            url: featureServerDeleteUrl,
            json: true,
            formData,
        }, function(error, response, body){
            if (error) reject(error);
            
            if (body) {
                resolve(body)
            } else {
                reject(body);
            }
        });
    });    
}

const appRouter = app => {

    app.get("/vehicle/:type", async function(req, res) {
        if (req.params.type === 'buses' ||
            req.params.type === 'ferries' ||
            req.params.type === 'lightrail' ||
            req.params.type === 'nswtrains' ||
            req.params.type === 'sydneytrains'
        ) {
            try {
                // 1. Get the vehicle position from TfNSW opendata
                const vehiclesPosition = await getVehiclesPosition(req.params.type);

                // 2. Create a CSV for that feed
                const filename = await createCSV(vehiclesPosition, req.params.type);

                // 3. Request tokens from ArcGIS online
                const token = await requestToken();

                // 4. Upload CSV
                const appendUploadId = await uploadCSV(filename, token);
                
                // 5. Finally run upsert with CSV
                const statusUrl = await upsert(appendUploadId, token);
                // console.log(statusUrl);

            } catch (e) {
                console.log(e);
            }
            
            res.status(200).send("ok");
        } else {
            res.status(200).send('Not querying: TfNSW Realtime Vehicle positions');
        } 
    });

    app.get("/delete", async function(req, res) {

        // Request tokens from ArcGIS online
        const token = await requestToken();

        await deleteVehicle(token);

        res.status(200).send('delete vehicle done');
    });

    app.get("/", function(req, res) {
        res.status(200).send('TfNSW Realtime Vehicle positions');
    });
}

module.exports = appRouter;