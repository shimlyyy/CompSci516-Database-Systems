db.movies.aggregate([
    {$addFields: {year: {$substr: ["$release_date", 7, -1]}}},
    {$match: {year: {"$exists": true, "$ne": ""}}},
    {$group: {_id: {summary:"$summary", year: "$year"}, count: {$sum: 1}}},
    {$project: {_id: 0, count: 1, year: "$_id.year", summary: "$_id.summary"}},
    {$sort: {year: 1, summary: -1}},
    {$limit: 100}
]).toArray()

