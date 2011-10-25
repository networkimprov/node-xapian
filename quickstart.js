var xapian = require('xapian');

var databases = new xapian.Database('db1');
databases.on('open', function(err) {
  if (err) throw err;

  var db2 = new xapian.Database('db2');
  db2.on('open', function(err) {
    if (err) throw err;
    databases.add_database(db2);
    var enquire = new xapian.Enquire(databases); // assumes no i/o
    var query = new xapian.Query(xapian.Query.OP_OR, 'term1', 'term2');
    console.log("Performing query [" + query.description + "]");
    enquire.set_query(query); // assumes no i/o

    enquire.get_mset(0, 10, function(err, mset) { // mset is an array
      if (err) throw err
      console.log(mset.length + " results found");
      iter(0);
      function iter(i) { // can't for-loop when depending on callback
        if (i === mset.length)
          return;

        mset[i].document.get_data(function(err, data) {
          if (err) throw err;
          console.log("Document ID " + mset[i].id + "\t" + mset[i].percent + "% [" + data + "]");
          iter(++i);
        });
      }
    });
  });
});

