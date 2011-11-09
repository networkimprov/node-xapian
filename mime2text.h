/* mime2text.h: convert common-format files to text for indexing
 *
 * Copyright 1999,2000,2001 BrightStation PLC
 * Copyright 2001,2005 James Aylett
 * Copyright 2001,2002 Ananova Ltd
 * Copyright 2002,2003,2004,2005,2006,2007,2008,2009,2010,2011 Olly Betts
 * Copyright 2009 Frank J Bruzzaniti
 * Copyright 2011 Liam Breck
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301
 * USA
 */

#ifndef OMEGA_INCLUDED_MIME2TEXT_H
#define OMEGA_INCLUDED_MIME2TEXT_H

#include <string>
#include <map>

namespace Xapian {

class Mime2Text {
public:
    Mime2Text(bool noexcl=false, int sampsize=512);

    // FIXME should tolower() inputs
    void setCommand(const char* iKey, const char* iVal) { commands[iKey] = iVal; }
    void setMimeType(const char* iKey, const char* iVal) { mime_map[iKey] = iVal; }

    enum Status {
        Status_OK,
        Status_TYPE,
        Status_IGNORE,
        Status_METATAG,
        Status_FILENAME,
        Status_FILTER,
        Status_COMMAND,
        Status_MD5,
        Status_TMPDIR
    };

    struct Fields {
        std::string author, title, sample, keywords, dump;
        std::string md5;
        std::string mimetype, command;
    };

    // if type NULL, check file ext, else if type starts with . find in mime_map
    Status convert(const char* filepath, const char* type, Fields* outFields);

protected:
    std::string shell_protect(const std::string& file);
    std::string file_to_string(const std::string& file);
    void get_pdf_metainfo(const std::string& safefile, std::string& author, std::string& title, std::string& keywords);
    void parse_pdfinfo_field(const char* p, const char* end, std::string& out, const char* field, size_t len);
    void generate_sample_from_csv(const std::string& csv_data, std::string& sample);

    bool ignore_exclusions;
    int sample_size;
    std::map<std::string, std::string> mime_map;
    std::map<std::string, std::string> commands;
};

}

#endif // OMEGA_INCLUDED_MIME2TEXT_H

