const express = require('express')
const app = express()
const port = 3000
const path = require("path");

let publicPath = path.resolve(__dirname)
app.use(express.static(publicPath))

app.listen(port, () => console.log(`Example app listening on port ${port}!`))

app.get('/create', create_table)
app.get('/destroy', destroy_table)
app.get('/query/:Year/:String/:Rating', query_table)

var AWS = require("aws-sdk");
const { S3, LexModelBuildingService, Mobile } = require('aws-sdk');
//const { arrayBuffer } = require('stream/consumers');
AWS.config.loadFromPath('./config.json');

var dynamodb = new AWS.DynamoDB();
var docClient = new AWS.DynamoDB.DocumentClient();


async function create_table(req,res) {
    try {
        AWS.config.setPromisesDependency();
        const s3 = new AWS.S3();
        const response = await s3.getObject({
            Bucket: 'csu44000assignment220',
            Key: 'moviedata.json'
        }).promise();

        var allMovies = JSON.parse(response.Body);
        var params = {
            TableName: "Movies",
            KeySchema: [
                { AttributeName: "year", KeyType: "HASH" },  //Partition key
                { AttributeName: "title", KeyType: "RANGE" }  //Sort key
            ],
            AttributeDefinitions: [
                { AttributeName: "year", AttributeType: "N" },
                { AttributeName: "title", AttributeType: "S" },
               // { AttributeName: "rating", AttributeType: "N" },
                //{ AttributeName: "rank", AttributeType: "S" },
                //{ AttributeName: "release_date", AttributeType: "S" }
            ],
            ProvisionedThroughput: {
                ReadCapacityUnits: 10,
                WriteCapacityUnits: 10
            }
        };
        dynamodb.createTable(params, function (err, data) {
            if (err) {
               console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
            } else {
                console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
            }
        });


        allMovies.forEach(function (movie) {
            var params = {
                TableName: "Movies",
                Item: {
                    "year": movie.year,
                    "title": movie.title,
                    "rating": movie.info.rating,
                    "rank": movie.info.rank,
                    "release_date": movie.info.release_date
                }
            };

            docClient.put(params, function (err, data) {
                if (err) {
                    console.error("Unable to add movie", movie.title, ". Error JSON:", JSON.stringify(err, null, 2));
                } else {
                    console.log("PutItem succeeded:", movie.title);
                }
            });
        });
    } catch (e) {
        console.log('our erro', e);
    }
}


async function destroy_table(req, res) {
    var params = {
        TableName: "Movies"
    };
    dynamodb.deleteTable(params, function (err, data) {
        if (err) {
            console.error("Unable to delete table. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            console.log("Deleted table. Table description JSON:", JSON.stringify(data, null, 2));
        }
    });
    console.log("Table Deleted")
}

async function query_table(req, res) {
    var params = {
        TableName: "Movies",
        ProjectionExpression: "#year, title, rating",
        FilterExpression: "#year = :yr and begins_with(title, :s) and rating > :threshold",
       // FilterExpression: "#year = :yr",

        ExpressionAttributeNames: {
            "#year": "year",
        },
        ExpressionAttributeValues: {
            ":yr": Number(req.params.Year),
            ":s": req.params.String,
            ":threshold": Number(req.params.Rating)
        }
    };

    console.log("Scanning Movies table.");
    docClient.scan(params, onScan);
    
    function onScan(err, data) {
        //make json array
        var return_movies = [];

        if (err) {
            console.error("Unable to scan the table. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            // print all the movies
            console.log("Scan succeeded.");
            data.Items.forEach(function (movie) {
                console.log(
                    movie.year + ": ",
                    movie.title, "- rating:", movie.rating);
                    return_movies.push({
                        "title" : movie.title,
                        "year" : movie.year,
                        "rating" : movie.rating
                    })
            });

            // continue scanning if we have more movies, because
            // scan can retrieve a maximum of 1MB of data
            if (typeof data.LastEvaluatedKey != "undefined") {
                console.log("Scanning for more...");
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                docClient.scan(params, onScan);
            }
        }
        console.log("Movies Queried")
        res.send(return_movies)
    }
}
