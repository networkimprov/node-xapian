
#include <xapian.h>
#include "mime2text.h"

#include <v8.h>
#include <node.h>
#include <node_events.h>
#include <node_buffer.h>

using namespace v8;
using namespace node;

static Persistent<String> kBusyMsg;

struct AsyncOpBase {
  AsyncOpBase(Handle<Function> cb)
    : callback(), error(NULL) {
    callback = Persistent<Function>::New(cb);
    ev_ref(EV_DEFAULT_UC);
  }
  virtual ~AsyncOpBase() {
    if (error) delete error;
    ev_unref(EV_DEFAULT_UC);
    callback.Dispose();
  }
  Persistent<Function> callback;
  Xapian::Error* error;
};

template <class T>
struct AsyncOp : public AsyncOpBase {
  AsyncOp(Handle<Object> ob, Handle<Function> cb)
    : AsyncOpBase(cb), object(ObjectWrap::Unwrap<T>(ob)) {
    if (object->mBusy)
      throw Exception::Error(kBusyMsg);
    object->mBusy = true;
    object->Ref();
  }
  virtual ~AsyncOp() { object->Unref(); }
  void poolDone() { object->mBusy = false; }
  T* object;
};

class Database : public EventEmitter {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

  Xapian::Database& getDb() { return *mDb; }

protected:
  Database() : EventEmitter(), mDb(NULL), mBusy(false) {}

  virtual ~Database() {
    if (mDb) {
      mDb->close();
      delete mDb;
    }
  }

  union {
    Xapian::Database* mDb;
    Xapian::WritableDatabase* mWdb;
  };
  bool mBusy;

  friend struct AsyncOp<Database>;

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> AddDatabase(const Arguments& args);

  static Handle<Value> Reopen(const Arguments& args);
  static int Open_pool(eio_req *req);
  static int Open_done(eio_req *req);
  struct Open_data : AsyncOp<Database> {
    Open_data(Handle<Object> ob, Handle<String> file, int wop=0)
      : AsyncOp<Database>(ob, Handle<Function>()), filename(file), writeopts(wop) {}
    String::Utf8Value filename;
    int writeopts;
  };
};

class WritableDatabase : public Database {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

  Xapian::WritableDatabase& getWdb() { return *mWdb; }

protected:
  WritableDatabase() : Database() { }

  ~WritableDatabase() { }

  friend struct AsyncOp<WritableDatabase>;

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> ReplaceDocument(const Arguments& args);
  static int AddDocument_pool(eio_req *req);
  static int AddDocument_done(eio_req *req);
  struct AddDocument_data : AsyncOp<WritableDatabase> {
    AddDocument_data(Handle<Object> ob, Handle<Function> cb, const Xapian::Document& doc, Handle<String> id)
      : AsyncOp<WritableDatabase>(ob, cb), document(doc), idterm(id) {}
    const Xapian::Document& document;
    Xapian::docid docid;
    String::Utf8Value idterm;
  };

  static Handle<Value> Commit(const Arguments& args);
  static Handle<Value> BeginTransaction(const Arguments& args);
  static Handle<Value> CommitTransaction(const Arguments& args);

  static int Commit_pool(eio_req *req);
  static int Commit_done(eio_req *req);
  struct Commit_data : AsyncOp<WritableDatabase> {
    Commit_data(Handle<Object> ob, Handle<Function> cb, int op, bool fl=false)
      : AsyncOp<WritableDatabase>(ob, cb), type(op), flush(fl) {}
    enum { eCommit, eBeginTx, eCommitTx };
    int type;
    bool flush;
  };
};

class TermGenerator : public ObjectWrap {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

  Xapian::TermGenerator mTg;

protected:
  TermGenerator() : ObjectWrap(), mTg() { }

  ~TermGenerator() { }

  friend struct Main_data;

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> SetDatabase(const Arguments& args);
  static Handle<Value> SetFlags(const Arguments& args);
  static Handle<Value> SetStemmer(const Arguments& args);
};

class Stem : public ObjectWrap {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

  Xapian::Stem mStem;

protected:
  Stem(const char* iLang) : ObjectWrap(), mStem(iLang) { }

  ~Stem() { }

  static Handle<Value> New(const Arguments& args);
};

class Enquire : public ObjectWrap {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

protected:
  Enquire(const Xapian::Database& iDb) : ObjectWrap(), mEnq(iDb), mBusy(false) {}

  ~Enquire() {
  }

  Xapian::Enquire mEnq;
  bool mBusy;

  friend struct AsyncOp<Enquire>;

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> SetQuery(const Arguments& args);

  static Handle<Value> GetMset(const Arguments& args);
  static int GetMset_pool(eio_req *req);
  static int GetMset_done(eio_req *req);
  struct GetMset_data : AsyncOp<Enquire> {
    GetMset_data(Handle<Object> ob, Handle<Function> cb, uint32_t fi, uint32_t mx)
      : AsyncOp<Enquire>(ob, cb), first(fi), maxitems(mx), set(NULL) {}
    ~GetMset_data() { if (set) delete [] set; }
    Xapian::doccount first, maxitems;
    struct Item {
      Xapian::docid id;
      Xapian::Document* document;
      Xapian::doccount rank, collapse_count;
      Xapian::weight weight;
      std::string collapse_key, description;
      Xapian::percent percent;
    };
    Item* set;
    int size;
  };
};

class Query : public ObjectWrap {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

  Xapian::Query mQry;

protected:
  template <class T>
  Query(Xapian::Query::op o, T a, T b) : ObjectWrap(), mQry(o, a, b) {}

  ~Query() {}

  static Handle<Value> New(const Arguments& args);

  //static Handle<Value> Fn(const Arguments& args);
};

class Document : public ObjectWrap {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

  Xapian::Document* getDoc() {
    return mDoc;
  }

protected:
  Document(Xapian::Document* iDoc) : ObjectWrap(), mDoc(iDoc), mBusy(false) {}

  ~Document() {
    delete mDoc;
  }

  Xapian::Document* mDoc;
  bool mBusy;

  friend struct AsyncOp<Document>;

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> GetData(const Arguments& args);
  static int GetData_pool(eio_req *req);
  static int GetData_done(eio_req *req);
  struct GetData_data : AsyncOp<Document> {
    GetData_data(Handle<Object> ob, Handle<Function> cb)
      : AsyncOp<Document>(ob, cb) {}
    std::string data;
  };
};

class Mime2Text : public ObjectWrap {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

  Xapian::Mime2Text m2T;

protected:
  Mime2Text() : ObjectWrap(), m2T(), mBusy(false) {}

  ~Mime2Text() { }

  bool mBusy;

  friend struct AsyncOp<Mime2Text>;
  friend struct Main_data;

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> Convert(const Arguments& args);
  static int Convert_pool(eio_req *req);
  static int Convert_done(eio_req *req);
  struct Convert_data : AsyncOp<Mime2Text> {
    Convert_data(Handle<Object> ob, Handle<Function> cb, Handle<String> fi, Handle<Value> ty)
      : AsyncOp<Mime2Text>(ob, cb), filename(fi), type(ty->IsNull() ? NULL : new String::Utf8Value(ty)) {}
    ~Convert_data() { if (type) delete type; }
    String::Utf8Value filename;
    String::Utf8Value* type;
    int status;
    Xapian::Mime2Text::Fields fields;
  };
};

static Handle<Value> AssembleDocument(const Arguments& args);
static int Main_pool(eio_req *req);
static int Main_done(eio_req *req);
struct Main_data : public AsyncOpBase {
  Main_data(Handle<Function> cb, Xapian::Document* doc, TermGenerator* tg, String::Utf8Value** tl, Mime2Text* m2t, Handle<Value> p, Handle<Value> m)
    : AsyncOpBase(cb), document(doc), termgen(tg), textlist(tl), mime2text(m2t), path(p), mimetype(m) {
    termgen->Ref();
    mime2text->Ref();
  }
  ~Main_data() {
    if (textlist) delete [] textlist;
    termgen->Unref();
    mime2text->Unref();
  }
  Xapian::Document* document;
  TermGenerator* termgen;
  String::Utf8Value** textlist;
  Mime2Text* mime2text;
  String::Utf8Value path;
  String::Utf8Value mimetype;
  Xapian::Mime2Text::Fields fields;
};

extern "C"
void init (Handle<Object> target) {
  HandleScope scope;
  kBusyMsg = Persistent<String>::New(String::New("object busy with async op"));
  Database::Init(target);
  WritableDatabase::Init(target);
  TermGenerator::Init(target);
  Stem::Init(target);
  Enquire::Init(target);
  Query::Init(target);
  Document::Init(target);
  Mime2Text::Init(target);
  target->Set(String::NewSymbol("assemble_document"), FunctionTemplate::New(AssembleDocument)->GetFunction());
}

static void tryCallCatch(Handle<Function> fn, Handle<Object> context, int argc, Handle<Value>* argv) {
  TryCatch try_catch;

  fn->Call(context, argc, argv);

  if (try_catch.HasCaught())
    FatalException(try_catch);
}

template <class T>
static T* GetInstance(Handle<Value> val) {
  if (val->IsObject() && T::constructor_template->HasInstance(val->ToObject()))
    return ObjectWrap::Unwrap<T>(val->ToObject());
  return NULL;
}

Persistent<FunctionTemplate> Database::constructor_template;

void Database::Init(Handle<Object> target) {
  constructor_template = Persistent<FunctionTemplate>::New(FunctionTemplate::New(New));
  constructor_template->Inherit(EventEmitter::constructor_template);
  constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
  constructor_template->SetClassName(String::NewSymbol("Database"));

  NODE_SET_PROTOTYPE_METHOD(constructor_template, "reopen", Reopen);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "add_database", AddDatabase);

  target->Set(String::NewSymbol("Database"), constructor_template->GetFunction());
}

Handle<Value> Database::New(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 1 || !args[0]->IsString())
    return ThrowException(Exception::TypeError(String::New("arguments are (string)")));

  Database* that = new Database();
  that->Wrap(args.This());

  eio_custom(Open_pool, EIO_PRI_DEFAULT, Open_done, new Open_data(args.This(), args[0]->ToString()));

  return args.This();
}

Handle<Value> Database::AddDatabase(const Arguments& args) {
  HandleScope scope;
  Database* aDb;
  if (args.Length() < 1 || !(aDb = GetInstance<Database>(args[0])))
    return ThrowException(Exception::TypeError(String::New("arguments are (Database)")));
  Database* that = ObjectWrap::Unwrap<Database>(args.This());
  if (that->mBusy)
    return ThrowException(Exception::Error(kBusyMsg));
  try {
  that->mDb->add_database(*aDb->mDb);
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }
  return Undefined();
}

Handle<Value> Database::Reopen(const Arguments& args) {
  HandleScope scope;

  Open_data* aData;
  try {
    aData = new Open_data(args.This(), Handle<String>());
  } catch (Local<Value> ex) {
    return ThrowException(ex);
  }

  eio_custom(Open_pool, EIO_PRI_DEFAULT, Open_done, aData);

  return Undefined();
}

int Database::Open_pool(eio_req *req) {
  Open_data* aData = (Open_data*) req->data;

  try {
  if (aData->object->mDb)
    aData->object->mDb->reopen();
  else
    aData->object->mDb = aData->writeopts ? new Xapian::WritableDatabase(*aData->filename, aData->writeopts) : new Xapian::Database(*aData->filename);
  } catch (const Xapian::Error& err) {
    aData->error = new Xapian::Error(err);
  }

  aData->poolDone();
  return 0;
}

int Database::Open_done(eio_req *req) {
  HandleScope scope;

  Open_data* aData = (Open_data*) req->data;

  Handle<Value> argv[1];
  if (aData->error)
    argv[0] = Exception::Error(String::New(aData->error->get_msg().c_str()));

  aData->object->Emit(String::New("open"), aData->error ? 1 : 0, argv);

  delete aData;

  return 0;
}

Persistent<FunctionTemplate> WritableDatabase::constructor_template;

void WritableDatabase::Init(Handle<Object> target) {
  constructor_template = Persistent<FunctionTemplate>::New(FunctionTemplate::New(New));
  constructor_template->Inherit(Database::constructor_template);
  constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
  constructor_template->SetClassName(String::NewSymbol("WritableDatabase"));

  NODE_SET_PROTOTYPE_METHOD(constructor_template, "replace_document", ReplaceDocument);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "commit", Commit);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "begin_transaction", BeginTransaction);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "commit_transaction", CommitTransaction);

  target->Set(String::NewSymbol("DB_OPEN"               ), Integer::New(Xapian::DB_OPEN               ), ReadOnly);
  target->Set(String::NewSymbol("DB_CREATE"             ), Integer::New(Xapian::DB_CREATE             ), ReadOnly);
  target->Set(String::NewSymbol("DB_CREATE_OR_OPEN"     ), Integer::New(Xapian::DB_CREATE_OR_OPEN     ), ReadOnly);
  target->Set(String::NewSymbol("DB_CREATE_OR_OVERWRITE"), Integer::New(Xapian::DB_CREATE_OR_OVERWRITE), ReadOnly);

  target->Set(String::NewSymbol("WritableDatabase"), constructor_template->GetFunction());
}

Handle<Value> WritableDatabase::New(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsInt32())
    return ThrowException(Exception::TypeError(String::New("arguments are (string, number)")));

  WritableDatabase* that = new WritableDatabase();
  that->Wrap(args.This());

  eio_custom(Open_pool, EIO_PRI_DEFAULT, Open_done, new Open_data(args.This(), args[0]->ToString(), args[1]->Int32Value()));

  return args.This();
}

Handle<Value> WritableDatabase::ReplaceDocument(const Arguments& args) {
  HandleScope scope;

  Document* aDoc;
  if (args.Length() < 3 || !args[0]->IsString() || !(aDoc = GetInstance<Document>(args[1])) || !args[2]->IsFunction())
    return ThrowException(Exception::TypeError(String::New("arguments are (string, Document, function)")));

  AddDocument_data* aData;
  try {
    aData = new AddDocument_data(args.This(), Local<Function>::Cast(args[2]), *aDoc->getDoc(), args[0]->ToString());
  } catch (Local<Value> ex) {
    return ThrowException(ex);
  }

  eio_custom(AddDocument_pool, EIO_PRI_DEFAULT, AddDocument_done, aData);

  return Undefined();
}

int WritableDatabase::AddDocument_pool(eio_req *req) {
  AddDocument_data* aData = (AddDocument_data*) req->data;

  try {
    if (aData->idterm.length())
      aData->docid = aData->object->mWdb->replace_document(*aData->idterm, aData->document);
    else
      aData->docid = aData->object->mWdb->add_document(aData->document);
  } catch (const Xapian::Error& err) {
    aData->error = new Xapian::Error(err);
  }

  aData->poolDone();
  return 0;
}

int WritableDatabase::AddDocument_done(eio_req *req) {
  HandleScope scope;

  AddDocument_data* aData = (AddDocument_data*) req->data;

  Handle<Value> argv[2];
  if (aData->error) {
    argv[0] = Exception::Error(String::New(aData->error->get_msg().c_str()));
  } else {
    argv[0] = Null();
    argv[1] = Integer::New(aData->docid);
  }

  tryCallCatch(aData->callback, aData->object->handle_, aData->error ? 1 : 2, argv);

  delete aData;

  return 0;
}

Handle<Value> WritableDatabase::Commit(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 1 || !args[0]->IsFunction())
    return ThrowException(Exception::TypeError(String::New("arguments are (function)")));
  Commit_data* aData;
  try {
    aData = new Commit_data(args.This(), Local<Function>::Cast(args[0]), Commit_data::eCommit);
  } catch (Local<Value> ex) {
    return ThrowException(ex);
  }

  eio_custom(Commit_pool, EIO_PRI_DEFAULT, Commit_done, aData);

  return Undefined();
}

Handle<Value> WritableDatabase::BeginTransaction(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 2 || !args[0]->IsBoolean() || !args[1]->IsFunction())
    return ThrowException(Exception::TypeError(String::New("arguments are (boolean, function)")));
  Commit_data* aData;
  try {
    aData = new Commit_data(args.This(), Local<Function>::Cast(args[1]), Commit_data::eBeginTx, args[0]->BooleanValue());
  } catch (Local<Value> ex) {
    return ThrowException(ex);
  }

  eio_custom(Commit_pool, EIO_PRI_DEFAULT, Commit_done, aData);

  return Undefined();
}

Handle<Value> WritableDatabase::CommitTransaction(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 1 || !args[0]->IsFunction())
    return ThrowException(Exception::TypeError(String::New("arguments are (function)")));
  Commit_data* aData;
  try {
    aData = new Commit_data(args.This(), Local<Function>::Cast(args[0]), Commit_data::eCommitTx);
  } catch (Local<Value> ex) {
    return ThrowException(ex);
  }

  eio_custom(Commit_pool, EIO_PRI_DEFAULT, Commit_done, aData);

  return Undefined();
}

int WritableDatabase::Commit_pool(eio_req *req) {
  Commit_data* aData = (Commit_data*) req->data;

  try {
    switch (aData->type) {
    case Commit_data::eCommit:   aData->object->mWdb->commit();                        break;
    case Commit_data::eBeginTx:  aData->object->mWdb->begin_transaction(aData->flush); break;
    case Commit_data::eCommitTx: aData->object->mWdb->commit_transaction();            break;
    }
  } catch (const Xapian::Error& err) {
    aData->error = new Xapian::Error(err);
  }

  aData->poolDone();
  return 0;
}

int WritableDatabase::Commit_done(eio_req *req) {
  HandleScope scope;

  Commit_data* aData = (Commit_data*) req->data;

  Handle<Value> argv[1];
  if (aData->error)
    argv[0] = Exception::Error(String::New(aData->error->get_msg().c_str()));

  tryCallCatch(aData->callback, aData->object->handle_, aData->error ? 1 : 0, argv);

  delete aData;

  return 0;
}

Persistent<FunctionTemplate> TermGenerator::constructor_template;

void TermGenerator::Init(Handle<Object> target) {
  constructor_template = Persistent<FunctionTemplate>::New(FunctionTemplate::New(New));
  constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
  constructor_template->SetClassName(String::NewSymbol("TermGenerator"));

  NODE_SET_PROTOTYPE_METHOD(constructor_template, "set_database", SetDatabase);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "set_flags", SetFlags);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "set_stemmer", SetStemmer);

  target->Set(String::NewSymbol("TermGenerator"), constructor_template->GetFunction());

  Handle<Object> aO = constructor_template->GetFunction();
  aO->Set(String::NewSymbol("FLAG_SPELLING"), Integer::New(Xapian::TermGenerator::FLAG_SPELLING), ReadOnly);
}

Handle<Value> TermGenerator::New(const Arguments& args) {
  HandleScope scope;
  if (args.Length())
    return ThrowException(Exception::TypeError(String::New("arguments are ()")));
  TermGenerator* that = new TermGenerator;
  that->Wrap(args.This());
  return args.This();
}

Handle<Value> TermGenerator::SetDatabase(const Arguments& args) {
  HandleScope scope;
  WritableDatabase* aDb;
  if (args.Length() < 1 || !(aDb = GetInstance<WritableDatabase>(args[0])))
    return ThrowException(Exception::TypeError(String::New("arguments are (Database)")));

  TermGenerator* that = ObjectWrap::Unwrap<TermGenerator>(args.This());
  try {
    that->mTg.set_database(aDb->getWdb());
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }

  return Undefined();
}

Handle<Value> TermGenerator::SetFlags(const Arguments& args) {
  HandleScope scope;
  if (args.Length() < 1 || !args[0]->IsInt32())
    return ThrowException(Exception::TypeError(String::New("arguments are (integer)")));

  TermGenerator* that = ObjectWrap::Unwrap<TermGenerator>(args.This());
  try {
    that->mTg.set_flags((Xapian::TermGenerator::flags)args[0]->Int32Value());
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }

  return Undefined();
}

Handle<Value> TermGenerator::SetStemmer(const Arguments& args) {
  HandleScope scope;
  Stem* aSt;
  if (args.Length() < 1 || !(aSt = GetInstance<Stem>(args[0])))
    return ThrowException(Exception::TypeError(String::New("arguments are (Stem)")));

  TermGenerator* that = ObjectWrap::Unwrap<TermGenerator>(args.This());
  try {
    that->mTg.set_stemmer(aSt->mStem);
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }

  return Undefined();
}

Persistent<FunctionTemplate> Stem::constructor_template;

void Stem::Init(Handle<Object> target) {
  constructor_template = Persistent<FunctionTemplate>::New(FunctionTemplate::New(New));
  constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
  constructor_template->SetClassName(String::NewSymbol("Stem"));

  //NODE_SET_PROTOTYPE_METHOD(constructor_template, "set_database", SetDatabase);

  target->Set(String::NewSymbol("Stem"), constructor_template->GetFunction());
}

Handle<Value> Stem::New(const Arguments& args) {
  HandleScope scope;
  if (args.Length() < 1 || !args[0]->IsString())
    return ThrowException(Exception::TypeError(String::New("arguments are (string)")));
  Stem* that;
  try {
    that = new Stem(*String::Utf8Value(args[0]));
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }
  that->Wrap(args.This());
  return args.This();
}

Persistent<FunctionTemplate> Enquire::constructor_template;

void Enquire::Init(Handle<Object> target) {
  constructor_template = Persistent<FunctionTemplate>::New(FunctionTemplate::New(New));
  constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
  constructor_template->SetClassName(String::NewSymbol("Enquire"));

  NODE_SET_PROTOTYPE_METHOD(constructor_template, "set_query", SetQuery);
  NODE_SET_PROTOTYPE_METHOD(constructor_template, "get_mset", GetMset);

  target->Set(String::NewSymbol("Enquire"), constructor_template->GetFunction());
}

Handle<Value> Enquire::New(const Arguments& args) {
  HandleScope scope;
  Database* aDb;
  if (args.Length() < 1 || !(aDb = GetInstance<Database>(args[0])))
    return ThrowException(Exception::TypeError(String::New("arguments are (Database)")));
  Enquire* that = new Enquire(aDb->getDb());
  that->Wrap(args.This());
  return args.This();
}

Handle<Value> Enquire::SetQuery(const Arguments& args) {
  HandleScope scope;
  Query* aQ;
  if (args.Length() < 1 || !(aQ = GetInstance<Query>(args[0])))
    return ThrowException(Exception::TypeError(String::New("arguments are (Query)")));
  Enquire* that = ObjectWrap::Unwrap<Enquire>(args.This());
  if (that->mBusy)
    return ThrowException(Exception::Error(kBusyMsg));
  try {
    that->mEnq.set_query(aQ->mQry);
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }
  return Undefined();
}

Handle<Value> Enquire::GetMset(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 3 || !args[0]->IsUint32() || !args[1]->IsUint32() || !args[2]->IsFunction())
    return ThrowException(Exception::TypeError(String::New("arguments are (number, number, function)")));
  GetMset_data* aData;
  try {
    aData = new GetMset_data(args.This(), Local<Function>::Cast(args[2]), args[0]->Uint32Value(), args[1]->Uint32Value());
  } catch (Local<Value> ex) {
    return ThrowException(ex);
  }

  eio_custom(GetMset_pool, EIO_PRI_DEFAULT, GetMset_done, aData);

  return Undefined();
}

int Enquire::GetMset_pool(eio_req *req) {
  GetMset_data* aData = (GetMset_data*) req->data;

  try {
  Xapian::MSet aSet = aData->object->mEnq.get_mset(aData->first, aData->maxitems);
  aData->set = new GetMset_data::Item[aSet.size()];
  aData->size = 0;
  for (Xapian::MSetIterator a = aSet.begin(); a != aSet.end(); ++a, ++aData->size) {
    aData->set[aData->size].id = *a;
    aData->set[aData->size].document = new Xapian::Document(a.get_document());
    aData->set[aData->size].rank = a.get_rank();
    aData->set[aData->size].collapse_count = a.get_collapse_count();
    aData->set[aData->size].weight = a.get_weight();
    aData->set[aData->size].collapse_key = a.get_collapse_key();
    aData->set[aData->size].description = a.get_description();
    aData->set[aData->size].percent = a.get_percent();
  }
  } catch (const Xapian::Error& err) {
    aData->error = new Xapian::Error(err);
  }

  aData->poolDone();
  return 0;
}

int Enquire::GetMset_done(eio_req *req) {
  HandleScope scope;

  GetMset_data* aData = (GetMset_data*) req->data;

  Handle<Value> argv[2];
  if (aData->error) {
    argv[0] = Exception::Error(String::New(aData->error->get_msg().c_str()));
  } else {
    argv[0] = Null();
    Local<Array> aList(Array::New(aData->size));
    Local<Function> aCtor(Document::constructor_template->GetFunction());
    for (int a = 0; a < aData->size; ++a) {
      Local<Object> aO(Object::New());
      Local<Value> aDoc[] = { External::New(aData->set[a].document) };
      aO->Set(String::NewSymbol("document"      ), aCtor->NewInstance(1, aDoc));
      aO->Set(String::NewSymbol("id"            ), Uint32::New(aData->set[a].id                  ));
      aO->Set(String::NewSymbol("rank"          ), Uint32::New(aData->set[a].rank                ));
      aO->Set(String::NewSymbol("collapse_count"), Uint32::New(aData->set[a].collapse_count      ));
      aO->Set(String::NewSymbol("weight"        ), Number::New(aData->set[a].weight              ));
      aO->Set(String::NewSymbol("collapse_key"  ), String::New(aData->set[a].collapse_key.c_str()));
      aO->Set(String::NewSymbol("description"   ), String::New(aData->set[a].description.c_str() ));
      aO->Set(String::NewSymbol("percent"       ),  Int32::New(aData->set[a].percent             ));
      aList->Set(a, aO);
    }
    argv[1] = aList;
  }

  tryCallCatch(aData->callback, aData->object->handle_, aData->error ? 1 : 2, argv);

  delete aData;

  return 0;
}

Persistent<FunctionTemplate> Query::constructor_template;

void Query::Init(Handle<Object> target) {
  constructor_template = Persistent<FunctionTemplate>::New(FunctionTemplate::New(New));
  constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
  constructor_template->SetClassName(String::NewSymbol("Query"));

  //NODE_SET_PROTOTYPE_METHOD(constructor_template, "fn", Fn);

  target->Set(String::NewSymbol("Query"), constructor_template->GetFunction());

  Handle<Object> aO = constructor_template->GetFunction();
  aO->Set(String::NewSymbol("OP_AND"         ), Integer::New(Xapian::Query::OP_AND         ), ReadOnly);
  aO->Set(String::NewSymbol("OP_OR"          ), Integer::New(Xapian::Query::OP_OR          ), ReadOnly);
  aO->Set(String::NewSymbol("OP_AND_NOT"     ), Integer::New(Xapian::Query::OP_AND_NOT     ), ReadOnly);
  aO->Set(String::NewSymbol("OP_XOR"         ), Integer::New(Xapian::Query::OP_XOR         ), ReadOnly);
  aO->Set(String::NewSymbol("OP_AND_MAYBE"   ), Integer::New(Xapian::Query::OP_AND_MAYBE   ), ReadOnly);
  aO->Set(String::NewSymbol("OP_FILTER"      ), Integer::New(Xapian::Query::OP_FILTER      ), ReadOnly);
  aO->Set(String::NewSymbol("OP_NEAR"        ), Integer::New(Xapian::Query::OP_NEAR        ), ReadOnly);
  aO->Set(String::NewSymbol("OP_PHRASE"      ), Integer::New(Xapian::Query::OP_PHRASE      ), ReadOnly);
  aO->Set(String::NewSymbol("OP_VALUE_RANGE" ), Integer::New(Xapian::Query::OP_VALUE_RANGE ), ReadOnly);
  aO->Set(String::NewSymbol("OP_SCALE_WEIGHT"), Integer::New(Xapian::Query::OP_SCALE_WEIGHT), ReadOnly);
  aO->Set(String::NewSymbol("OP_ELITE_SET"   ), Integer::New(Xapian::Query::OP_ELITE_SET   ), ReadOnly);
  aO->Set(String::NewSymbol("OP_VALUE_GE"    ), Integer::New(Xapian::Query::OP_VALUE_GE    ), ReadOnly);
  aO->Set(String::NewSymbol("OP_VALUE_LE"    ), Integer::New(Xapian::Query::OP_VALUE_LE    ), ReadOnly);
  aO->Set(String::NewSymbol("OP_SYNONYM"     ), Integer::New(Xapian::Query::OP_SYNONYM     ), ReadOnly);
}

Handle<Value> Query::New(const Arguments& args) {
  HandleScope scope;
  int aN;
  std::vector<std::string> aList;
  for (aN = 1; aN < args.Length() && args[aN]->IsString(); ++aN)
    aList.push_back(*String::Utf8Value(args[aN]));
  if (args.Length() < 2 || !args[0]->IsInt32() || aN < args.Length())
    return ThrowException(Exception::TypeError(String::New("arguments are (Query.op, string ...)")));
  Query* that;
  const char* aDesc;
  try {
  that = new Query((Xapian::Query::op)args[0]->Int32Value(), aList.begin(), aList.end());
  aDesc = that->mQry.get_description().c_str();
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }
  args.This()->Set(String::NewSymbol("description"), String::New(aDesc));
  that->Wrap(args.This());
  return args.This();
}

Persistent<FunctionTemplate> Document::constructor_template;

void Document::Init(Handle<Object> target) {
  constructor_template = Persistent<FunctionTemplate>::New(FunctionTemplate::New(New));
  constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
  constructor_template->SetClassName(String::NewSymbol("Document"));

  NODE_SET_PROTOTYPE_METHOD(constructor_template, "get_data", GetData);

  target->Set(String::NewSymbol("Document"), constructor_template->GetFunction());
}

Handle<Value> Document::New(const Arguments& args) {
  HandleScope scope;

  if (args.Length() && !args[0]->IsExternal())
    return ThrowException(Exception::TypeError(String::New("arguments are ()")));

  Document* that = new Document(args.Length() ? (Xapian::Document*) External::Unwrap(args[0]) : new Xapian::Document);
  that->Wrap(args.This());

  return args.This();
}

Handle<Value> Document::GetData(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 1 || !args[0]->IsFunction())
    return ThrowException(Exception::TypeError(String::New("arguments are (function)")));
  GetData_data* aData;
  try {
    aData = new GetData_data(args.This(), Local<Function>::Cast(args[0]));
  } catch (Local<Value> ex) {
    return ThrowException(ex);
  }

  eio_custom(GetData_pool, EIO_PRI_DEFAULT, GetData_done, aData);

  return Undefined();
}

int Document::GetData_pool(eio_req *req) {
  GetData_data* aData = (GetData_data*) req->data;

  try {
  aData->data = aData->object->mDoc->get_data();
  } catch (const Xapian::Error& err) {
    aData->error = new Xapian::Error(err);
  }

  aData->poolDone();
  return 0;
}

int Document::GetData_done(eio_req *req) {
  HandleScope scope;

  GetData_data* aData = (GetData_data*) req->data;

  Handle<Value> argv[2];
  if (aData->error) {
    argv[0] = Exception::Error(String::New(aData->error->get_msg().c_str()));
  } else {
    argv[0] = Null();
    argv[1] = String::New(aData->data.c_str());
  }

  tryCallCatch(aData->callback, aData->object->handle_, aData->error ? 1 : 2, argv);

  delete aData;

  return 0;
}

Persistent<FunctionTemplate> Mime2Text::constructor_template;

void Mime2Text::Init(Handle<Object> target) {
  constructor_template = Persistent<FunctionTemplate>::New(FunctionTemplate::New(New));
  constructor_template->InstanceTemplate()->SetInternalFieldCount(1);
  constructor_template->SetClassName(String::NewSymbol("Mime2Text"));

  NODE_SET_PROTOTYPE_METHOD(constructor_template, "convert", Convert);

  target->Set(String::NewSymbol("Mime2Text"), constructor_template->GetFunction());
}

Handle<Value> Mime2Text::New(const Arguments& args) {
  HandleScope scope;

  if (args.Length())
    return ThrowException(Exception::TypeError(String::New("arguments are ()")));

  Mime2Text* that = new Mime2Text();
  that->Wrap(args.This());

  return args.This();
}

Handle<Value> Mime2Text::Convert(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 3 || !args[0]->IsString() || (!args[1]->IsString() && !args[1]->IsNull()) || !args[2]->IsFunction())
    return ThrowException(Exception::TypeError(String::New("arguments are (string, string|null, function)")));
  Convert_data* aData;
  try {
    aData = new Convert_data(args.This(), Local<Function>::Cast(args[2]), args[0]->ToString(), args[1]);
    aData->poolDone(); // concurrent access ok
  } catch (Local<Value> ex) {
    return ThrowException(ex);
  }

  eio_custom(Convert_pool, EIO_PRI_DEFAULT, Convert_done, aData);

  return Undefined();
}

int Mime2Text::Convert_pool(eio_req *req) {
  Convert_data* aData = (Convert_data*) req->data;

  try {
  aData->status = aData->object->m2T.convert(*aData->filename, aData->type ? **aData->type : NULL, &aData->fields);
  } catch (const Xapian::Error& err) {
    aData->error = new Xapian::Error(err);
  }

  return 0;
}

int Mime2Text::Convert_done(eio_req *req) {
  HandleScope scope;

  Convert_data* aData = (Convert_data*) req->data;

  Handle<Value> argv[3];
  if (aData->error) {
    argv[0] = Exception::Error(String::New(aData->error->get_msg().c_str()));
  } else {
    argv[0] = Null();
    argv[1] = Integer::New(aData->status);
    Local<Object> aO(Object::New());
    aO->Set(String::NewSymbol("title"   ), String::New(aData->fields.title.c_str()));
    aO->Set(String::NewSymbol("author"  ), String::New(aData->fields.author.c_str()));
    aO->Set(String::NewSymbol("keywords"), String::New(aData->fields.keywords.c_str()));
    aO->Set(String::NewSymbol("sample"  ), String::New(aData->fields.sample.c_str()));
    aO->Set(String::NewSymbol("body"    ), String::New(aData->fields.dump.c_str()));
    aO->Set(String::NewSymbol("md5"     ), String::New(aData->fields.md5.c_str()));
    aO->Set(String::NewSymbol("mimetype"), String::New(aData->fields.mimetype.c_str()));
    aO->Set(String::NewSymbol("command" ), String::New(aData->fields.command.c_str()));
    argv[2] = aO;
  }

  tryCallCatch(aData->callback, aData->object->handle_, aData->error ? 1 : 3, argv);

  delete aData;

  return 0;
}


/*
document input object: {
  // all members optional; at least one required
  id_term: string, // boolean term; if found in index, replace/delete that document
  data: string, // pass to Document::set_data()
  text: [ string/buffer, ... ], // pass to TermGenerator::index_text()
  file: { path: string, mime_t: string, ... }, // invoke format converter library, then index_text()
  terms: { term: wdfinc, ... }, // pass to Document::add_term()
  values: { slot: value, ... } // pass to Document::add_value()
}
*/

static Handle<Value> AssembleDocument(const Arguments& args) {
  HandleScope scope;

  TermGenerator* aTg;
  Mime2Text* aM2t;
  if (args.Length() < 4 || !(aTg = GetInstance<TermGenerator>(args[0])) || !(aM2t = GetInstance<Mime2Text>(args[1]))
   || !args[2]->IsObject() || !args[3]->IsFunction())
    return ThrowException(Exception::TypeError(String::New("arguments are (TermGenerator, Mime2Text, object, function)")));

  Local<Object> aO = args[2]->ToObject();
  Local<String> aKey;
  Local<Value> aVal, aPath, aMime;
  String::Utf8Value** aTextList = NULL;
  Xapian::Document aDoc;
  try {
    if (aO->Has(aKey = String::New("id_term"))) {
      aVal = aO->Get(aKey);
      if (!aVal->IsString())
        return ThrowException(Exception::TypeError(String::New("input object id_term not a string")));
      aDoc.add_boolean_term(*String::Utf8Value(aVal));
    }
    if (aO->Has(aKey = String::New("data"))) {
      aVal = aO->Get(aKey);
      if (!aVal->IsString())
        return ThrowException(Exception::TypeError(String::New("input object data not a string")));
      aDoc.set_data(*String::Utf8Value(aVal));
    }
    if (aO->Has(aKey = String::New("file"))) {
      aVal = aO->Get(aKey);
      if (!aVal->IsObject())
        return ThrowException(Exception::TypeError(String::New("input object file not an object")));
      Local<Object> aFile = aVal->ToObject();
      aPath = aFile->Get(String::New("path"));
      if (!aPath->IsString())
        return ThrowException(Exception::TypeError(String::New("input object file.path not a string")));
      if (aFile->Has(aKey = String::New("mime_t"))) {
        aMime = aFile->Get(aKey);
        if (!aMime->IsString())
          return ThrowException(Exception::TypeError(String::New("input object file.mime_t not a string")));
      }
    }
    if (aO->Has(aKey = String::New("terms"))) {
      aVal = aO->Get(aKey);
      if (!aVal->IsObject())
        return ThrowException(Exception::TypeError(String::New("input object terms not an object")));
      Local<Object> aTerms = aVal->ToObject();
      Local<Array> aNames = aTerms->GetPropertyNames();
      for (uint32_t a = 0; a < aNames->Length(); ++a) {
        aVal = aTerms->Get(aKey = aNames->Get(a)->ToString());
        if (aVal->IsUint32())
          aDoc.add_term(*String::Utf8Value(aKey), aVal->Uint32Value());
      }
    }
    if (aO->Has(aKey = String::New("values"))) {
      aVal = aO->Get(aKey);
      if (!aVal->IsObject())
        return ThrowException(Exception::TypeError(String::New("input object values not an object")));
      Local<Object> aValues = aVal->ToObject();
      Local<Array> aNames = aValues->GetPropertyNames();
      for (uint32_t a = 0; a < aNames->Length(); ++a) {
        aVal = aNames->Get(a);
        if (aVal->IsUint32()) {
          uint32_t aSlot = aVal->Uint32Value();
          aVal = aValues->Get(aSlot);
          if (aVal->IsString())
            aDoc.add_value(aSlot, *String::Utf8Value(aVal));
        }
      }
    }
    if (aO->Has(aKey = String::New("text"))) {
      aVal = aO->Get(aKey);
      if (!aVal->IsArray())
        return ThrowException(Exception::TypeError(String::New("input object text not an array")));
      Local<Array> aAry = Local<Array>::Cast(aVal);
      aTextList = new String::Utf8Value*[aAry->Length()+1];
      for (uint32_t a = 0; a < aAry->Length(); ++a)
        aTextList[a] = new String::Utf8Value(aAry->Get(a));
      aTextList[aAry->Length()] = NULL;
    }
    if (aVal.IsEmpty())
      return ThrowException(Exception::TypeError(String::New("input object has no relevant members")));
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }

  Main_data* aData = new Main_data(Local<Function>::Cast(args[3]), new Xapian::Document(aDoc), aTg, aTextList, aM2t, aPath, aMime);

  eio_custom(Main_pool, EIO_PRI_DEFAULT, Main_done, aData);

  return Undefined();
}

static int Main_pool(eio_req *req) {
  Main_data* aData = (Main_data*) req->data;

  try {
  aData->termgen->mTg.set_document(*aData->document);
  for (int a = 0; aData->textlist && aData->textlist[a]; ++a) {
    aData->termgen->mTg.index_text(**aData->textlist[a]);
    aData->termgen->mTg.increase_termpos();
  }
  if (aData->path.length()) {
    int aStatus = aData->mime2text->m2T.convert(*aData->path, aData->mimetype.length() ? *aData->mimetype : NULL, &aData->fields);
    if (aStatus != Xapian::Mime2Text::Status_OK) {
      std::string aMsg("Mime2Text::convert error: ");
      aMsg += (char) (aStatus + '0');
      throw Xapian::InternalError(aMsg);
    }
    if (!aData->fields.title.empty()) {
      aData->termgen->mTg.index_text(aData->fields.title);
      aData->termgen->mTg.increase_termpos();
    }
    if (!aData->fields.author.empty()) {
      aData->termgen->mTg.index_text(aData->fields.author);
      aData->termgen->mTg.increase_termpos();
    }
    if (!aData->fields.keywords.empty()) {
      aData->termgen->mTg.index_text(aData->fields.keywords);
      aData->termgen->mTg.increase_termpos();
    }
    if (!aData->fields.dump.empty()) {
      aData->termgen->mTg.index_text(aData->fields.dump);
      aData->termgen->mTg.increase_termpos();
    }
  }
  } catch (const Xapian::Error& err) {
    aData->error = new Xapian::Error(err);
  }

  return 0;
}

static int Main_done(eio_req *req) {
  HandleScope scope;

  Main_data* aData = (Main_data*) req->data;

  Handle<Value> argv[2];
  if (aData->error) {
    argv[0] = Exception::Error(String::New(aData->error->get_msg().c_str()));
    delete aData->document;
  } else {
    argv[0] = Null();
    Local<Value> aDoc[] = { External::New(aData->document) };
    argv[1] = Document::constructor_template->GetFunction()->NewInstance(1, aDoc);
  }

  tryCallCatch(aData->callback, Context::GetCurrent()->Global(), aData->error ? 1 : 2, argv);

  delete aData;

  return 0;
}


