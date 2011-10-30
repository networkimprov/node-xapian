var xapian = require('./xapian-binding');

var aDocs = [
  {data:'doc one', text:'text one two three four five six'},
  {data:'item new', text:'text four five six seven eight nine'},
  {data:'more here', text:'text alpha beta gamma delta'},
  {data:'then some', text:'text gulf alpha charlie'},
  {data:'and two', text:'text gulf stream waters'},
  {data:'something more', text:'text six ten eleven twelve'}
];


var atg = new xapian.TermGenerator;
var stem = new xapian.Stem('english');
atg.set_stemmer(stem);

makeDb('db1');
function makeDb(path) {
  var wdb = new xapian.WritableDatabase(path, xapian.DB_CREATE_OR_OVERWRITE);
  wdb.on('open', function(err) {
    if (err) throw err;
    console.log('opened WritableDatabase');
    atg.set_database(wdb);
    atg.set_flags(xapian.TermGenerator.FLAG_SPELLING);
    fAdd(0);
    function fAdd(n) {
      if (n < aDocs.length) {
        wdb.add_document(atg, aDocs[n], function(err) {
          if (err) throw err;
          console.log('added "'+aDocs[n].data+'"');
          fAdd(++n);
        });
        return;
      }
      wdb.commit(function(err) {
        if (err) throw err;
        console.log('committed '+path);
        if (path === 'db1')
          makeDb('db2');
        else {
          wdb = null;
          fRead();
        }
      });
    }
  });
}

function fRead() {
  var databases = new xapian.Database('db1');
  databases.on('open', function(err) {
    if (err) throw err;
    console.log('opened Database');

    var db2 = new xapian.Database('db2');
    db2.on('open', function(err) {
      if (err) throw err;
      databases.add_database(db2);
      var enquire = new xapian.Enquire(databases); // assumes no i/o
      var query = new xapian.Query(xapian.Query.OP_OR, 'one', 'six');
      console.log("Performing query [" + query.description + "]");
      enquire.set_query(query); // assumes no i/o

      enquire.get_mset(0, 10, function(err, mset) { // mset is an array
        if (err) throw err
        console.log(mset.length + " results found");
        console.log(require('util').inspect(mset));
        iter(0);
        function iter(i) { // can't for-loop when depending on callback
          if (i < mset.length) {
            mset[i].document.get_data(function(err, data) {
              if (err) throw err;
              console.log("Document ID " + mset[i].id + "\t" + mset[i].percent + "% [" + data + "]");
              iter(++i);
            });
            return;
          }
          databases = null;
          console.log('done');
        }
      });
    });
  });
}

