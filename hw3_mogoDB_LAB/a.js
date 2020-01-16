db.movies.find({"genre.Animation": true, release_date:/.*1995.*/}).count()
