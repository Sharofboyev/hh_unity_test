const mongoose = require("mongoose");
const config = require("./config");
const first = require("./first.json");
const second = require("./second.json");
const {validate1, validate2} = require("./schemas")

//exercise 1. Connecting to local database from localhost. Database name defined in config.js
mongoose.connect(`mongodb://localhost/${config.DB.db_name}`, { useUnifiedTopology: true, useNewUrlParser: true }).then(async () => {
    console.log("Connected to the database...");
    //deleting collections university1 and university2 if exists
    mongoose.connection.db.listCollections({}, {nameOnly: true}).toArray().then(async (collections) => {
        for (let i = 0; i < collections.length; i++){
            if (collections[i].name == "university1") await mongoose.connection.db.dropCollection("university1")
            else if (collections[i].name == "university2") await mongoose.connection.db.dropCollection("university2")
        }

        //all exercises done in main function
        main().catch((err) => {
            console.log(err.message)
        })
    })
}).catch((err) => {
    console.error("Can't connect to the database...", err);
})

const universitySchema1 = mongoose.Schema({         //Schema for the first.json file and it's further model
    country: String,
    city: String,
    name: String,
    location: {
        ll: [ Number ]
    },
    students: [{
        year: Number,
        number: Number
    }],
    seconds: [{
        difference: Number
    }],
    longitude: Number,
    latitude: Number,
    count_difference: Number
});

const universitySchema2 = mongoose.Schema({             //schema for the second.json
    country: String,
    overallStudents: Number
})

const University1 = mongoose.model("University1", universitySchema1);

const University2 = mongoose.model("University2", universitySchema2)

async function insertData1 (data){          // asynchronous function for inserting document to the university1 collection
    const {error, value} = validate1(data)
    if (error) return console.error(error.details[0].message);
    const university = new University1(value);
    await university.save();
}

async function insertData2 (data){          // asynchronous function for inserting document to the university2 collection
    const {error, value} = validate2(data)
    if (error) return console.error(error.details[0].message);
    const university = new University2(value);
    await university.save();
}

async function update1(filter, data) {          // function for update filtered documents in collection university1
    const {error, value} = validate1(data)
    if (error) return console.error(error.details[0].message);
    const university = await University1.updateMany(filter, {
        $set: value,
    },{new: true, useFindAndModify: false});
    if (!university) return console.error("No university with given id")
}

async function main() {
    //Exercise 2. Inserting all data
    for (let i = 0; i < first.length; i++){
        await insertData1(first[i]);
    }

    for (let i = 0; i < second.length; i++){
        await insertData2(second[i]);
    }

    //Exercise 3. Extracting longitude and latitude
    let docs = await University1.aggregate([
        {
            $sort: {
                country: 1
            }
        }, 
        {
            $project: {
                _id: true,
                location: true
            }
        }
    ])
    for (let i = 0; i < docs.length; i++){
        await update1({_id: docs[i]._id}, {
            latitude: docs[i].location.ll[0],
            longitude: docs[i].location.ll[1] 
        })
    }
    //Exercise 4. Finding difference between all students in the last year and the overall students from the same country
    docs = await University1.aggregate([
        {
          $project: {
            country: '$country',
            current_students: {
              $filter: {
                input: '$students', 
                as: 'student', 
                cond: {
                  $eq: [
                    '$$student.year', {
                      $max: '$students.year'
                    }
                  ]
                }
              }
            }
          }
        }, {
          $project: {
            country: true, 
            all: {
              $sum: '$current_students.number'
            }
          }
        },{
            $lookup: {
                from: 'university2',
                localField: 'country',
                foreignField: 'country',
                as: 'overall'
            }
        },
        {
            $sort: {
                _id: 1
            }
        }
    ])

    for (let i = 0; i < docs.length; i++){
        await update1({_id: docs[i]._id}, {
            count_difference: docs[i].all - docs[i].overall[0].overallStudents
        })
    }

    //Exercise 5. Find documents count by countries
    docs = await University1.aggregate([
        { 
            $group: {
                _id: "$country",
                count: {
                    $sum: 1
                }
            }
        },
        {
            $sort: {
                _id: 1
            }
        }
    ])
    docs.forEach((value, index) => {
        console.log(value)
    })
    mongoose.connection.close()
}