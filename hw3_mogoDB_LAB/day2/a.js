db.movies.updateMany({rating: {$gte: 4}}, {$set: {"summary": "great"}}),
db.movies.updateMany({rating: {$gte:2, $lt: 4}}, {$set:{"summary": "ok"}}),
db.movies.updateMany({rating: {$lt: 2}}, {$set:{"summary": "bad"}}),
db.movies.aggregate([
{$group: {_id: {summary: "$summary"}, count: {$sum:1} } },
{$project: {_id:0, summary:  "$_id.summary", count: 1} },
{$sort: {count: 1}}
]).toArray()

