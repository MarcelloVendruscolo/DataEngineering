var mapper = function() {
    var pronouns = ["HAN", "HON", "HEN", 'DEN', 'DET', 'DENNA', 'DENNE'];
    db.tweets.createIndex({ text: "text" });
    if (this.text) {
        for (var counter = 0; counter < pronouns.length; counter++) {
            var match = 0;
            match = this.find( { $text: { $search: "\"" +pronouns[counter]+ "\"" } }).count();
            if (match !== 0) {
                emit(pronouns[counter], 1);
            } else {
                emit(pronouns[counter], 0);
            }
        }
        emit('tweets_count', 1);
    }
}
var reducer = function (key,values) {
    return Array.sum(values);
}
db.tweets.mapReduce(mapper, reducer, { query: { }, out: "pronouns_count" })
db.pronouns_count.find()