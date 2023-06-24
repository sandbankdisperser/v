module sqlite

import time

$if freebsd || openbsd {
	#flag -I/usr/local/include
	#flag -L/usr/local/lib
}
$if windows {
	#flag windows -I@VEXEROOT/thirdparty/sqlite
	#flag windows -L@VEXEROOT/thirdparty/sqlite
	#flag windows @VEXEROOT/thirdparty/sqlite/sqlite3.o
} $else {
	#flag -lsqlite3
}

#include "sqlite3.h"

// https://www.sqlite.org/rescode.html
pub const (
	sqlite_ok                 = 0
	sqlite_error              = 1
	sqlite_row                = 100
	sqlite_done               = 101
	sqlite_cantopen           = 14
	sqlite_ioerr_read         = 266
	sqlite_ioerr_short_read   = 522
	sqlite_ioerr_write        = 778
	sqlite_ioerr_fsync        = 1034
	sqlite_ioerr_fstat        = 1802
	sqlite_ioerr_delete       = 2570

	// Documnted as not used, but required as we return it in query_one if there is no result
	sqlite_empty              = 16
	sqlite_range              = 25
	sqlite_open_main_db       = 0x00000100
	sqlite_open_temp_db       = 0x00000200
	sqlite_open_transient_db  = 0x00000400
	sqlite_open_main_journal  = 0x00000800
	sqlite_open_temp_journal  = 0x00001000
	sqlite_open_subjournal    = 0x00002000
	sqlite_open_super_journal = 0x00004000
	sqlite_open_wal           = 0x00080000
)

pub enum SyncMode {
	off
	normal
	full
}

pub enum JournalMode {
	off
	delete
	truncate
	persist
	memory
}

struct C.sqlite3 {
}

struct C.sqlite3_stmt {
}

[heap]
pub struct Stmt {
	stmt &C.sqlite3_stmt = unsafe { nil }
	db   &DB = unsafe { nil }
}

struct SQLError {
	MessageError
}

//
[heap]
pub struct DB {
pub mut:
	is_open bool
mut:
	conn &C.sqlite3 = unsafe { nil }
}

// str returns a text representation of the DB
pub fn (db &DB) str() string {
	return 'sqlite.DB{ conn: ' + ptr_str(db.conn) + ' }'
}

pub struct Row {
pub mut:
	columns []string
	vals    []string
}

//
fn C.sqlite3_open(&char, &&C.sqlite3) int

fn C.sqlite3_close(&C.sqlite3) int

fn C.sqlite3_busy_timeout(db &C.sqlite3, ms int) int

fn C.sqlite3_last_insert_rowid(&C.sqlite3) i64

//
fn C.sqlite3_prepare_v2(&C.sqlite3, &char, int, &&C.sqlite3_stmt, &&char) int

fn C.sqlite3_step(&C.sqlite3_stmt) int

fn C.sqlite3_finalize(&C.sqlite3_stmt) int

//
fn C.sqlite3_column_name(&C.sqlite3_stmt, int) &char

fn C.sqlite3_column_text(&C.sqlite3_stmt, int) &u8

fn C.sqlite3_column_int(&C.sqlite3_stmt, int) int

fn C.sqlite3_column_int64(&C.sqlite3_stmt, int) i64

fn C.sqlite3_column_double(&C.sqlite3_stmt, int) f64

fn C.sqlite3_column_count(&C.sqlite3_stmt) int

//
fn C.sqlite3_errstr(int) &char

fn C.sqlite3_errmsg(&C.sqlite3) &char

fn C.sqlite3_errcode(&C.sqlite3) int

fn C.sqlite3_mprintf(&char, ...voidprt) &char

fn C.sqlite3_free(voidptr)

fn C.sqlite3_changes(&C.sqlite3) int

// connect Opens the connection with a database.
// connect Opens the connection with a database.
pub fn connect(path string) !DB {
	db := &C.sqlite3(unsafe { nil })
	code := C.sqlite3_open(&char(path.str), &db)
	if code != 0 {
		return &SQLError{
			msg: unsafe { cstring_to_vstring(&char(C.sqlite3_errstr(code))) }
			code: code
		}
	}
	return DB{
		conn: db
		is_open: true
	}
}

// close Closes the DB.
// TODO: For all functions, determine whether the connection is
// closed first, and determine what to do if it is
pub fn (mut db DB) close() !bool {
	code := C.sqlite3_close(db.conn)
	if code == 0 {
		db.is_open = false
	} else {
		return &SQLError{
			msg: unsafe { cstring_to_vstring(&char(C.sqlite3_errstr(code))) }
			code: code
		}
	}
	return true // successfully closed
}

// Only for V ORM
fn get_int_from_stmt(stmt &C.sqlite3_stmt) int {
	x := C.sqlite3_step(stmt)
	if x != C.SQLITE_OK && x != C.SQLITE_DONE {
		C.puts(C.sqlite3_errstr(x))
	}

	res := C.sqlite3_column_int(stmt, 0)
	C.sqlite3_finalize(stmt)
	return res
}

// last_insert_rowid returns last inserted rowid
// https://www.sqlite.org/c3ref/last_insert_rowid.html
pub fn (db &DB) last_insert_rowid() i64 {
	return C.sqlite3_last_insert_rowid(db.conn)
}

// get_affected_rows_count returns `sqlite changes()` meaning amount of rows affected by most recent sql query
pub fn (db &DB) get_affected_rows_count() int {
	return C.sqlite3_changes(db.conn)
}

// q_int returns a single integer value, from the first column of the result of executing `query`
pub fn (db &DB) q_int(query string) !int {
	stmt := &C.sqlite3_stmt(unsafe { nil })
	defer {
		C.sqlite3_finalize(stmt)
	}
	C.sqlite3_prepare_v2(db.conn, &char(query.str), query.len, &stmt, 0)
	code := C.sqlite3_step(stmt)
	if code != sqlite.sqlite_row {
		if code != sqlite.sqlite_done {
			return db.error_message(code, query)
		}
	}
	res := C.sqlite3_column_int(stmt, 0)
	return res
}

// q_string returns a single string value, from the first column of the result of executing `query`
pub fn (db &DB) q_string(query string) !string {
	stmt := &C.sqlite3_stmt(unsafe { nil })
	defer {
		C.sqlite3_finalize(stmt)
	}
	C.sqlite3_prepare_v2(db.conn, &char(query.str), query.len, &stmt, 0)
	code := C.sqlite3_step(stmt)
	if code != sqlite.sqlite_row {
		if code != sqlite.sqlite_done {
			return db.error_message(code, query)
		}
	}
	val := unsafe { &u8(C.sqlite3_column_text(stmt, 0)) }
	return if val != &u8(0) { unsafe { tos_clone(val) } } else { '' }
}

// exec executes the query on the given `db`, and returns an array of all the results, alongside any result code.
[manualfree]
pub fn (db &DB) exec(query string) ![]Row {
	stmt := &C.sqlite3_stmt(unsafe { nil })
	defer {
		C.sqlite3_finalize(stmt)
	}

	mut res := C.sqlite3_prepare_v2(db.conn, &char(query.str), query.len, &stmt, 0)
	if res != sqlite.sqlite_ok {
		return db.error_message(res, query)
	}

	nr_cols := C.sqlite3_column_count(stmt)
	mut rows := []Row{}
	for {
		res = C.sqlite3_step(stmt)
		// Result Code SQLITE_ROW; Another row is available
		if res != sqlite.sqlite_row {
			break
		}
		mut row := Row{}
		for i in 0 .. nr_cols {
			name := unsafe { &char(C.sqlite3_column_name(stmt, i)) }
			row.columns << unsafe { cstring_to_vstring(name) }

			val := unsafe { &u8(C.sqlite3_column_text(stmt, i)) }
			if val == &u8(0) {
				row.vals << ''
			} else {
				row.vals << unsafe { tos_clone(val) }
			}
		}
		rows << row
	}
	if res != sqlite.sqlite_ok && res != sqlite.sqlite_done {
		return db.error_message(res, query)
	}
	return rows
}

// exec_one executes a query on the given `db`.
// It returns either the first row from the result or an error
[manualfree]
pub fn (db &DB) exec_one(query string) !Row {
	rows := db.exec(query) or { return err }
	defer {
		unsafe { rows.free() }
	}
	if rows.len == 0 {
		return SQLError{
			msg: 'No rows'
			code: sqlite.sqlite_empty
		}
	}
	res := rows[0]
	return res
}

// exec_code executes a query, and returns the integer SQLite result code.
// use it, in case you don't expect any row results and need the result code.
// otherwise use the exec_none function which returns nothing or an error
pub fn (db &DB) exec_code(query string) int {
	stmt := &C.sqlite3_stmt(unsafe { nil })
	C.sqlite3_prepare_v2(db.conn, &char(query.str), query.len, &stmt, 0)
	code := C.sqlite3_step(stmt)
	C.sqlite3_finalize(stmt)
	return code
}

// exec_none executes a query and on failure returns an SQLError error with the sqlite error code and message
pub fn (db &DB) exec_none(query string) ! {
	stmt := &C.sqlite3_stmt(unsafe { nil })
	C.sqlite3_prepare_v2(db.conn, &char(query.str), query.len, &stmt, 0)
	code := C.sqlite3_step(stmt)
	if code != sqlite.sqlite_ok && code != sqlite.sqlite_done {
		err := SQLError{
			msg: unsafe { cstring_to_vstring(&char(C.sqlite3_errmsg(db.conn))) }
			code: code
		}
		C.sqlite3_finalize(stmt)
		return err
	}
	C.sqlite3_finalize(stmt)
}

[manualfree]
pub fn escape_str(str string) string {
	escaped_buffer := unsafe {
		cstring_to_vstring(&char(C.sqlite3_mprintf(&char('%q'.str), voidptr(&char(str.str)))))
	}
	escaped := '${escaped_buffer}'
	unsafe { escaped_buffer.free() }
	return escaped
}

// error_message returns a proper V error, given an integer error code received from SQLite, and a query string
[manualfree]
pub fn (db &DB) error_message(code int, query string) IError {
	errmsg := unsafe { cstring_to_vstring(&char(C.sqlite3_errmsg(db.conn))) }
	msg := '${errmsg} (${code}) (${query})'
	unsafe { errmsg.free() }
	return SQLError{
		msg: msg
		code: code
	}
}

pub type Primitive = bool | f32 | f64 | i16 | i64 | i8 | int | string | u16 | u32 | u64 | u8

// executes a query and replace every instance of ? with a quoted and escaped version of parms
pub fn (db &DB) exec_params(query string, params ...Primitive) ![]Row {
	if query.count('?') != params.len {
		return SQLError{
			msg: 'parameter count invalid: ${params.len} != ${query.count('?')}}'
			code: sqlite.sqlite_range
		}
	}
	mut escaped_query := query
	for _, param in params {
		// We only need to escape strings
		match param {
			i8, i16, int, u8, u16, u32, bool {
				escaped_query = escaped_query.replace_once('?', '${int(param)}')
			}
			i64, u64 {
				escaped_query = escaped_query.replace_once('?', '${i64(param)}')
			}
			f32, f64 {
				escaped_query = escaped_query.replace_once('?', '${f64(param)}')
			}
			string {
				escaped_query = escaped_query.replace_once('?', "'${escape_str(param)}'")
			}
		}
	}
	return db.exec(escaped_query)
}

// TODO pub fn (db &DB) exec_param(query string, param string) []Row {

// create_table issues a "create table if not exists" command to the db.
// It creates the table named 'table_name', with columns generated from 'columns' array.
// The default columns type will be TEXT.
pub fn (db &DB) create_table(table_name string, columns []string) ! {
	return db.exec_none('create table if not exists ${table_name} (' + columns.join(',\n') + ')')
}

// busy_timeout sets a busy timeout in milliseconds.
// Sleeps for a specified amount of time when a table is locked. The handler
// will sleep multiple times until at least "ms" milliseconds of sleeping have accumulated.
// (see https://www.sqlite.org/c3ref/busy_timeout.html)
pub fn (db &DB) busy_timeout(ms int) int {
	return C.sqlite3_busy_timeout(db.conn, ms)
}

// synchronization_mode sets disk synchronization mode, which controls how
// aggressively SQLite will write data to physical storage.
// .off: No syncs at all. (fastest)
// .normal: Sync after each sequence of critical disk operations.
// .full: Sync after each critical disk operation (slowest).
pub fn (db &DB) synchronization_mode(sync_mode SyncMode) ! {
	if sync_mode == .off {
		return db.exec_none('pragma synchronous = OFF;')
	} else if sync_mode == .full {
		return db.exec_none('pragma synchronous = FULL;')
	} else {
		return db.exec_none('pragma synchronous = NORMAL;')
	}
}

// journal_mode controls how the journal file is stored and processed.
// .off: No journal record is kept. (fastest)
// .memory: Journal record is held in memory, rather than on disk.
// .delete: At the conclusion of a transaction, journal file is deleted.
// .truncate: Journal file is truncated to a length of zero bytes.
// .persist: Journal file is left in place, but the header is overwritten to indicate journal is no longer valid.
pub fn (db &DB) journal_mode(journal_mode JournalMode) ! {
	if journal_mode == .off {
		return db.exec_none('pragma journal_mode = OFF;')
	} else if journal_mode == .delete {
		return db.exec_none('pragma journal_mode = DELETE;')
	} else if journal_mode == .truncate {
		return db.exec_none('pragma journal_mode = TRUNCATE;')
	} else if journal_mode == .persist {
		return db.exec_none('pragma journal_mode = PERSIST;')
	} else if journal_mode == .memory {
		return db.exec_none('pragma journal_mode = MEMORY;')
	} else {
		return db.exec_none('pragma journal_mode = MEMORY;')
	}
}
