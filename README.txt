For Node v0.4.x

Mirrors Xapian API closely, except:
  Enquire::get_mset returns an Array, not an iterator
  WritableDatabase::add_document takes a document parameters object, and can replace a document

Todo:

Test on Node v0.6

Rename WritableDatabaese::add_document
Integrate mime-file converter from omindex
Support more fields in add_document parameters object

Surface begin_transaction/commit_transaction

