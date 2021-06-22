const mongoose = require("mongoose");
const config = require("./config");
const first = require("./first.json");
const second = require("./second.json");
const {validate1, validate2} = require("./schemas")
mongoose.connect(`mongodb://localhost/${config.DB.db_name}`, { useUnifiedTopology: true, useNewUrlParser: true }).then(() => {
    console.log("Connected to the database...");
    mongoose.connection.db.dropCollection("university1", function(err, result) {
        if (err) console.log("Error occured while dropping table university1: ", err.message)
        console.log("University1 collection dropped");
        mongoose.connection.db.dropCollection("university2", function(err, result) {
            if (err) console.log("Error occured while dropping table university2: ", err.message)
            console.log("University2 collection dropped");

            main();
        });
    });
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
    const result = await university.save();
    //console.log("Inserted document to University1 : ", result)
}

async function insertData2 (data){          // asynchronous function for inserting document to the university2 collection
    const {error, value} = validate2(data)
    if (error) return console.error(error.details[0].message);
    const university = new University2(value);
    const result = await university.save();
    //console.log("Inserted document to University2 : ", result)
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
    for (let i = 0; i < first.length; i++){
        await insertData1(first[i]);
    }

    for (let i = 0; i < second.length; i++){
        await insertData2(second[i]);
    }

    University1.aggregate([{
        $sort: {
            country: 1
        }
    }, {
        $project: {
            _id: true,
            location: true
        }
    }]).then(docs => {
        console.log(docs[0])
        docs.forEach(async (value) => {
            await update1({_id: value._id}, {
                latitude: value.location.ll[0],
                longitude: value.location.ll[1] 
            })
        })
    }).catch((err) => {
        console.log(err.message);
    })

    University1.aggregate([
        {
          '$project': {
            'country': '$country', 
            'current_students': {
              '$filter': {
                'input': '$students', 
                'as': 'student', 
                'cond': {
                  '$eq': [
                    '$$student.year', {
                      '$max': '$students.year'
                    }
                  ]
                }
              }
            }
          }
        }, {
          '$project': {
            'country': true, 
            'all': {
              '$sum': '$current_students.number'
            }
          }
        }, {
          '$group': {
            '_id': '$country', 
            'all_students': {
              '$sum': '$all'
            }
          }
        },{
            '$sort': {
                '_id': 1
            }
        }
      ]).then(docs => {
        console.log(docs)
        docs.forEach(async (value, index) => {

        })
    }).catch((err) => {
        console.log(err.message);
    })
    University2.find((err, docs) => {
        if (err) return console.log(err.message);
        docs.forEach(async (value, index) => {
            const university = await University1.findOne({country: value.country}, {}, {useFindAndModify: false})
            if (university){
                await update1({_id: university._id}, {
                    count_difference: value.overallStudents - university.students.length
                })
            }
        })
    })
}
/*
insertion().then(() => {
    console.log("All data inserted...")
}).catch(err => {
    console.error("Cannot insert all data...", err)                                                //should be called only once
})             
*/

