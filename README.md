# tfnsw-ago-lambda
AWS Lambda for TfNSW Realtime Public Transport Feed to ArcGIS online

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

### Prerequisites

Get your application api key from [TfNSW](https://opendata.transport.nsw.gov.au)

The app can be deploy as standalone NodeJS web app or as a AWS lambda function. To deploy to AWS Lambda, use [Serverless](https://serverless.com/)

### Installing

```
npm install
```

### Credentials and Registering your App

For this code to work, you need to
[add](http://doc.arcgis.com/en/marketplace/provider/add-item-to-agol.htm) and
[register](http://doc.arcgis.com/en/marketplace/provider/register-app.htm) an app in ArcGIS Online,
add the correct redirect URI (e.g. `http://localhost:8080`), and add the client ID and secret to [/routes/routes.js](/routes/routes.js).

* [How to add an app in ArcGIS Online](http://doc.arcgis.com/en/marketplace/provider/add-item-to-agol.htm)
* [How to register an app in ArcGIS Online](http://doc.arcgis.com/en/marketplace/provider/register-app.htm)
* Make sure to set the correct redirect URI (e.g. `http://localhost:8080`)

Finally, update [/routes/routes.js](/routes/routes.js) to contain your client ID and secret (and portal URL if not ArcGIS Online):

```javascript
const client_id = 'xx';
const client_secret = 'xx';
```

Update the feature server URL

```javascript
const featureServerUrl = 'xx';
```

And lastly, update the api key you got from TfNSW Open Data

```javascript
headers: { 'authorization':'apikey xxxx' }
```

### Deploy to AWS Lambda

```
sls deploy
```

### Running the Lambda function offline

```
sls offline start
```

## To run as a NodeJS app

Comment out the Serverless handle and uncomment the local server part in [/index.js](/index.js)

```javascript
// module.exports.handler = serverless(app);

// USE THIS FOR LOCAL SERVER
var server = app.listen(3000, function () {
    console.log("app running on port.", server.address().port);
});
```

## Authors

* **Lucas Lew** - *Initial work*

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc
