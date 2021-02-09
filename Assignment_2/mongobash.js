conn = new Mongo();
db = conn.getDB('tweets_db');

var mapper = function() {
    var map_pronouns = new Map()
    var list_pronouns = ['han', 'hon', 'hen', 'den', 'det', 'denna', 'denne'];
    list_pronouns.forEach(function(pronoun) {
        map_pronouns.set(pronoun, 0);
    })
    var tweet_text = this.text
    for (let pronoun of map_pronouns.keys()) {
        var match = 0;
        let re = new RegExp('\\b' +pronoun+ '\\b', 'i');
        match = tweet_text.match(re)
        if (match !== null) {
            map_pronouns.set(pronoun, 1)
        }
        emit(pronoun, map_pronouns.get(pronoun));
    }
    emit('tweets_count', 1);
}
var reducer = function (key,values) {
    return Array.sum(values);
}
db.tweets.mapReduce(mapper, reducer, { query: { }, out: "pronouns_count" })
db.pronouns_count.find().pretty()