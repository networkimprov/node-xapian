var xapian = require('./xapian-binding');

var aDocs = [
  {data:'doc one',   text:['text one two three four five six'],    terms:{max:1},         values:{1:'stuff'}, id_term:'#dk83ndj'},
  {data:'doc one',   text:['text one two three four five six'],    terms:{max:1},         values:{1:'stuff'}, id_term:'#dk83ndj'},
  {data:'item new',  text:['text four five six seven eight nine'], terms:{min:2},         values:{0:'thing'}},
  {data:'more here', text:['text alpha beta gamma delta'],         terms:{min:1, max:2},  values:{3:'hello'}},
  {data:'then some', text:['text gulf alpha charlie'],             terms:{max:1},         values:{1:'hi', 2:'you'}},
  {data:'and two',   text:['text gulf stream waters'],             terms:{min:1},         values:{4:'a', 5:'b'}},
  {data:'something', text:['text six ten eleven twelve'],          terms:{max:1},         values:{0:'what'}, file:{path:'mime-test.html'}}
];


var m2t = new xapian.Mime2Text;
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
    wdb.begin_transaction(true, function(err) {
      if (err) throw err;
      fAdd(0);
      function fAdd(n) {
        if (n < aDocs.length) {
          xapian.assemble_document(atg, m2t, aDocs[n], function(err, doc) {
            if (err) throw err;
            wdb.replace_document(aDocs[n].id_term||'', doc, function(err) {
              if (err) throw err;
              console.log('added "'+aDocs[n].data+'"');
              fAdd(++n);
            });
          });
          return;
        }
        wdb.commit_transaction(function(err) {
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
      var query = new xapian.Query(xapian.Query.OP_OR, 'one', 'six', 'min');
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
          m2t.convert('mime-test.html', null, function(err, result) {
            if (err) throw err;
            console.log(result.title+' '+result.body);
          });
        }
      });
    });
  });
}

