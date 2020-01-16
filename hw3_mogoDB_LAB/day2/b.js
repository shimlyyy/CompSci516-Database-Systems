db.movies.aggregate( [
{$match: {"genre.Adventure":true}},
{$group: {_id: {movie_title: "$movie_title"}, average_rating:{$avg:"$rating"}, count:{$sum:1} }},
{$match : {count :{$gte: 100}}},
{$project: {_id:0, movie_title: "$_id.movie_title", average_rating:1}},
{$sort: {average_rating: -1}},
{$limit:20} ]).toArray()
