﻿updater = require('./lib/es-update');

var qryObj = {
  "query": {
    "match_all" : {}
  }
}

var serverOptions = {
    host: 'localhost',
    port: 9200
};

var fieldToMatch = "aField";

var stringToMatch = "A_badddd_String";

var replacementString = "a_better_string";

var index = "my_index";

var type = "my_type";

updater.esUpdate(qryObj, index, type, serverOptions, fieldToMatch, stringToMatch, replacementString,  function(err){
    console.log(err)
 })