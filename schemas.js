const Joi = require("joi")
module.exports.validate1 = function validate1(data) {       //data validator for university1 collection
    return Joi.object({
        country: Joi.string(),
        city: Joi.string(),
        name: Joi.string(),
        location: Joi.object({
            ll: Joi.array().items(Joi.number().required()).length(2)
        }),
        students: Joi.array().items(
            Joi.object({
                year: Joi.number(),
                number: Joi.number()
            })
        ),
        seconds: Joi.array().items(Joi.object({
            difference: Joi.number()
        })).optional(),
        longitude: Joi.number().optional(),
        latitude: Joi.number().optional(),
        count_difference: Joi.number().optional()
    }).validate(data)
}

module.exports.validate2 = function validate2(data) {       //data validator for university2 collection
    return Joi.object({
        country: Joi.string(),
        overallStudents: Joi.number()
    }).validate(data)
}