db.flights_per_airplane.findOne({TailNum: 'N249AU'})

db.flights_per_airplane.ensureIndex({TailNum: 1})

db.flights_per_airplane.findOne({TailNum: 'N249AU'})
