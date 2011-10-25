
#include <xapian.h>

#include <v8.h>
#include <node.h>
#include <node_events.h>
#include <node_buffer.h>

using namespace v8;
using namespace node;


template <class T>
struct AsyncOp {
  AsyncOp(Handle<Object> ob, Handle<Function> cb);
  virtual ~AsyncOp();
  T* object;
  Persistent<Function> callback;
  Xapian::Error* error;
};

class Database : public EventEmitter {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

  Xapian::Database& getDb() { return *mDb; }

protected:
  Database() : EventEmitter(), mDb(NULL), mOpening(false) {}

  ~Database() {
    if (mDb) {
      mDb->close();
      delete mDb;
    }
  }

  Xapian::Database* mDb;
  bool mOpening;

  friend struct AsyncOp<Database>;

  static Handle<Value> New(const Arguments& args);

  static Handle<Value> AddDatabase(const Arguments& args);

  static Handle<Value> Reopen(const Arguments& args);
  static int Open_pool(eio_req *req);
  static int Open_done(eio_req *req);
  struct Open_data : AsyncOp<Database> {
    Open_data(Handle<Object> ob, Handle<String> file)
      : AsyncOp<Database>(ob, Handle<Function>()), filename(file) {}
    String::Utf8Value filename;
  };
};

class Enquire : public ObjectWrap {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

protected:
  Enquire(const Xapian::Database& iDb) : ObjectWrap(), mEnq(iDb) {}

  ~Enquire() {
  }

  Xapian::Enquire mEnq;

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

  Xapian::Query& getQry() { return mQry; }

protected:
  template <class T>
  Query(Xapian::Query::op o, T a, T b) : ObjectWrap(), mQry(o, a, b) {}

  ~Query() {}

  Xapian::Query mQry;

  static Handle<Value> New(const Arguments& args);

  //static Handle<Value> Fn(const Arguments& args);
};

class Document : public ObjectWrap {
public:
  static void Init(Handle<Object> target);

  static Persistent<FunctionTemplate> constructor_template;

  void setDoc(Xapian::Document* iDoc) {
    if (mDoc) delete mDoc;
    mDoc = iDoc;
  }

protected:
  Document(Xapian::Document* iDoc) : ObjectWrap(), mDoc(iDoc) {}

  ~Document() {
    if (mDoc) delete mDoc;
  }

  Xapian::Document* mDoc;

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

extern "C"
void init (Handle<Object> target) {
  Database::Init(target);
  Enquire::Init(target);
  Query::Init(target);
  Document::Init(target);
}

template <class T>
AsyncOp<T>::AsyncOp(Handle<Object> ob, Handle<Function> cb)
  : object(ObjectWrap::Unwrap<T>(ob)), callback(Persistent<Function>::New(cb)), error(NULL) {
  ev_ref(EV_DEFAULT_UC);
  object->Ref();
}

template <class T>
AsyncOp<T>::~AsyncOp() {
  if (error) delete error;
  callback.Dispose();
  object->Unref();
  ev_unref(EV_DEFAULT_UC);
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
  HandleScope scope;

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
  that->mOpening = true;
  that->Wrap(args.This());

  eio_custom(Open_pool, EIO_PRI_DEFAULT, Open_done, new Open_data(args.This(), args[0]->ToString()));

  return args.This();
}

Handle<Value> Database::AddDatabase(const Arguments& args) {
  HandleScope scope;
  Database* aDb;
  if (args.Length() < 1 || (aDb = GetInstance<Database>(args[0])))
    return ThrowException(Exception::TypeError(String::New("arguments are (Database)")));
  Database* that = ObjectWrap::Unwrap<Database>(args.This());
  try {
  that->mDb->add_database(*aDb->mDb);
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }
  return Undefined();
}

Handle<Value> Database::Reopen(const Arguments& args) {
  HandleScope scope;

  Database* that = ObjectWrap::Unwrap<Database>(args.This());
  if (that->mOpening)
    return ThrowException(Exception::Error(String::New("database open in progress")));
  that->mOpening = true;

  eio_custom(Open_pool, EIO_PRI_DEFAULT, Open_done, new Open_data(args.This(), Handle<String>()));

  return Undefined();
}

int Database::Open_pool(eio_req *req) {
  Open_data* aData = (Open_data*) req->data;

  try {
  if (aData->object->mDb)
    aData->object->mDb->reopen();
  else
    aData->object->mDb = new Xapian::Database(*aData->filename);
  } catch (const Xapian::Error& err) {
    aData->error = new Xapian::Error(err);
  }

  return 0;
}

int Database::Open_done(eio_req *req) {
  HandleScope scope;

  Open_data* aData = (Open_data*) req->data;

  Local<Value> argv[1];
  if (aData->error)
    argv[0] = Exception::Error(String::New(aData->error->get_msg().c_str()));

  aData->object->mOpening = false;

  aData->object->Emit(String::New("open"), aData->error ? 1 : 0, argv);

  delete aData;

  return 0;
}

Persistent<FunctionTemplate> Enquire::constructor_template;

void Enquire::Init(Handle<Object> target) {
  HandleScope scope;

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
  if (args.Length() < 1 || (aDb = GetInstance<Database>(args[0])))
    return ThrowException(Exception::TypeError(String::New("arguments are (Database)")));
  Enquire* that = new Enquire(aDb->getDb());
  that->Wrap(args.This());
  return args.This();
}

Handle<Value> Enquire::SetQuery(const Arguments& args) {
  HandleScope scope;
  Query* aQ;
  if (args.Length() < 1 || (aQ = GetInstance<Query>(args[0])))
    return ThrowException(Exception::TypeError(String::New("arguments are (Query)")));
  Enquire* that = ObjectWrap::Unwrap<Enquire>(args.This());
  try {
    that->mEnq.set_query(aQ->getQry());
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }
  return Undefined();
}

Handle<Value> Enquire::GetMset(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 3 || !args[0]->IsUint32() || !args[1]->IsUint32() || !args[2]->IsFunction())
    return ThrowException(Exception::TypeError(String::New("arguments are (number, number, function)")));

  eio_custom(GetMset_pool, EIO_PRI_DEFAULT, GetMset_done,
    new GetMset_data(args.This(), Local<Function>::Cast(args[2]), args[0]->Uint32Value(), args[1]->Uint32Value()));

  return Undefined();
}

int Enquire::GetMset_pool(eio_req *req) {
  GetMset_data* aData = (GetMset_data*) req->data;

  try {
  Xapian::MSet aSet = aData->object->mEnq.get_mset(aData->first, aData->maxitems);
  aData->set = new GetMset_data::Item[aSet.size()];
  aData->size = 0;
  for (Xapian::MSetIterator a = aSet.begin(); a != aSet.end(); ++a, ++aData->size) {
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

  return 0;
}

int Enquire::GetMset_done(eio_req *req) {
  HandleScope scope;

  GetMset_data* aData = (GetMset_data*) req->data;

  Local<Value> argv[2];
  if (aData->error) {
    argv[0] = Exception::Error(String::New(aData->error->get_msg().c_str()));
  } else {
    argv[0] = Local<Value>::New(Null());
    Local<Array> aList(Array::New(aData->size));
    for (int a = 0; a < aData->size; ++a) {
      Local<Object> aO(Object::New());
      Local<Value> aDoc[] = { External::New(aData->set[a].document) };
      aO->Set(String::NewSymbol("document"      ), Document::constructor_template->GetFunction()->NewInstance(1, aDoc));
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
  HandleScope scope;

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
  Local<String> aDesc;
  try {
  that = new Query((Xapian::Query::op)args[0]->Int32Value(), aList.begin(), aList.end());
  aDesc = String::New(that->mQry.get_description().c_str());
  } catch (const Xapian::Error& err) {
    return ThrowException(Exception::Error(String::New(err.get_msg().c_str())));
  }
  args.This()->Set(String::NewSymbol("description"), aDesc);
  that->Wrap(args.This());
  return args.This();
}

Persistent<FunctionTemplate> Document::constructor_template;

void Document::Init(Handle<Object> target) {
  HandleScope scope;

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

  Document* that = new Document(args.Length() ? (Xapian::Document*) External::Unwrap(args[0]) : NULL);
  that->Wrap(args.This());

  return args.This();
}

Handle<Value> Document::GetData(const Arguments& args) {
  HandleScope scope;

  if (args.Length() < 1 || !args[1]->IsFunction())
    return ThrowException(Exception::TypeError(String::New("arguments are (function)")));

  eio_custom(GetData_pool, EIO_PRI_DEFAULT, GetData_done, new GetData_data(args.This(), Local<Function>::Cast(args[0])));

  return Undefined();
}

int Document::GetData_pool(eio_req *req) {
  GetData_data* aData = (GetData_data*) req->data;

  try {
  aData->data = aData->object->mDoc->get_data();
  } catch (const Xapian::Error& err) {
    aData->error = new Xapian::Error(err);
  }

  return 0;
}

int Document::GetData_done(eio_req *req) {
  HandleScope scope;

  GetData_data* aData = (GetData_data*) req->data;

  Local<Value> argv[2];
  if (aData->error) {
    argv[0] = Exception::Error(String::New(aData->error->get_msg().c_str()));
  } else {
    argv[0] = Local<Value>::New(Null());
    argv[1] = String::New(aData->data.c_str());
  }

  tryCallCatch(aData->callback, aData->object->handle_, aData->error ? 1 : 2, argv);

  delete aData;

  return 0;
}


