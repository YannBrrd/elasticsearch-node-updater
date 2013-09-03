var ElasticSearchClient = require('elasticsearchclient');

var esClient;
var stringToMatch;
var replacementString;
var keyToMatch;
var updateCount = 0;

function replacer(key, value) {
    
    if(key === keyToMatch) {
        console.log(value.replace(stringToMatch, replacementString, "gi"));
        return value.replace(stringToMatch, replacementString, "gi");
    }
    return value;
}

function updateQuery(qStart, qSize, query) {
    newQuery = query;
    
    newQuery['from'] = qStart;
    newQuery['size'] = qSize;  
  
    return newQuery;
}

function fireUpdate(index, type, toUpdate) {
    
    var updates = toUpdate;
    
    console.log("Firing updates : " + updates.length / 2);
   
    
    esClient.bulk(updates, {})
        .on('data', function(data) {
            var response = JSON.parse(data);
            if(response.error) {
                console.log("Errors in bulk indexing...");
                console.log(response);
            } else {
                console.log("Indexed " + response.items.length);
            }
        })
        .on('done', function(done){
        })
        .on('error', function(error){
            console.log("-------------Error-----------------");
            console.log(JSON.parse(error));
            console.log("-------------------------------------");
        })
        .exec();
}

function updateEntries(index, type, qryObj, stringToMatch, replacementString) {
   
    esClient.search(index, type, qryObj, function(err, data){
        if(err)
            throw err;

        var esresponse = JSON.parse(data);
            
        if(esresponse.hits) {
            
            var commands = [];
          
            for (hit in esresponse.hits.hits) {
                entryId = esresponse.hits.hits[hit]._id;
                entry = esresponse.hits.hits[hit]._source;
               
                var newEntry = JSON.stringify(entry, replacer);
               
                //bulk preparation
                commands.push({ "index" : { "_index" :index, "_type" : type, "_id" :entryId} });
                commands.push(JSON.parse(newEntry));
                updateCount++;
                
                if (commands.length > 1000) {
                    //bulk index
                    fireUpdate(index, type, commands.splice(0, 1000));
                }
            }
            
            if (commands.length > 0)
                fireUpdate(index, type, commands);
        }
    });
}

var esUpdate = function(qryObj, index, type, serverOptions, aKey, aStringToMatch, aReplacementString) {
    
    var maxHits = 0;
    
    stringToMatch = aStringToMatch;
    replacementString = aReplacementString;
    keyToMatch = aKey;
    
    esClient = new ElasticSearchClient(serverOptions);
    
    // First get maxHits count
    esClient.search(index, type, qryObj, function (err, data) {
        if(err)
            throw err;

        var esresponse = JSON.parse(data);
        
        if(esresponse.hits) {
            
            maxHits = esresponse.hits.total;
            console.log("Found " + maxHits + " matching hits.");
        
            //Then update
            for ( var i = 0 ; i < maxHits; i = i + 500)
                updateEntries(index, type, updateQuery(i, i + 500, qryObj), stringToMatch, replacementString);

        } else {
            console.log(esresponse);
            console.log("No matching hit.");
        }

    });
}


exports.esUpdate = esUpdate;