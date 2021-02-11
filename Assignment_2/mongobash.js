conn = new Mongo();
db = conn.getDB('tweets_db');

var mapper = function() {
    var tweet_text = this.text;
    var list_pronouns = [{pronoun:'han', count:0}, {pronoun:'hon', count:0}, {pronoun:'hen', count:0}, {pronoun:'den', count:0}, {pronoun:'det', count:0}, {pronoun:'denna', count:0}, {pronoun:'denne', count:0}];

    list_pronouns.forEach(function(object) {
        var re = new RegExp(object.pronoun, 'i');
        var match = tweet_text.match(re);
        if (match !== null) {
            object.count = 1;
        }
        emit(object.pronoun, object.count);
    })
    emit('tweets_count', 1);
}

var reducer = function (key,values) {
    return Array.sum(values);
}

db.tweets.mapReduce(mapper, reducer, { query: { }, out: "pronouns_count" });
printjson(db.pronouns_count.find().toArray());