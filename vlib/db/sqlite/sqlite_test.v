import db.sqlite

type Connection = sqlite.DB

struct User {
pub:
	id   int    [primary; sql: serial]
	name string
}

type Content = []u8 | string

struct Host {
pub:
	db Connection
}

fn (back Host) get_users() []User {
	return []
}

fn create_host(db Connection) !Host {
	sql db {
		create table User
	}!

	return Host{
		db: db
	}
}

fn test_sqlite() {
	$if !linux {
		return
	}
	mut db := sqlite.connect(':memory:') or {
		println(err)
		assert false
		return
	}
	assert db.is_open
	defer {
		db.close() or {
			println(err)
			assert false
		}
		assert !db.is_open
	}

	db.exec('drop table if exists users') or {
		println(err)
		assert false
		return
	}
	db.exec("create table users (id integer primary key, name text default '');") or {
		println(err)
		assert false
		return
	}
	db.exec("insert into users (name) values ('Sam')") or {
		println(err)
		assert false
		return
	}
	assert db.last_insert_rowid() == 1
	assert db.get_affected_rows_count() == 1
	db.exec("insert into users (name) values ('Peter')") or {
		println(err)
		assert false
		return
	}
	assert db.last_insert_rowid() == 2
	db.exec("insert into users (name) values ('Kate')") or {
		println(err)
		assert false
		return
	}
	assert db.last_insert_rowid() == 3
	nr_users := db.q_int('select count(*) from users') or {
		println(err)
		assert false
		return
	}
	assert nr_users == 3
	name := db.q_string('select name from users where id = 1') or {
		println(err)
		assert false
		return
	}
	assert name == 'Sam'

	// this insert will be rejected due to duplicated id
	if rows := db.exec("insert into users (id,name) values (1,'Sam')") {
		panic('insert succedded when it should have failed')
	}
	assert db.get_affected_rows_count() == 0

	users := db.exec('select * from users') or {
		println(err)
		assert false
		return
	}
	assert users.len == 3

	db.exec_none('vacuum') or {
		println(err)
		assert false
		return
	}

	user := db.exec_one('select * from users where id = 3') or {
		println(err)
		assert false
		return
	}
	assert user.vals.len == 2

	db.exec("update users set name='zzzz' where name='qqqq'") or {
		println(err)
		assert false
		return
	}
	assert db.get_affected_rows_count() == 0

	db.exec("update users set name='Peter1' where name='Peter'") or {
		println(err)
		assert false
		return
	}
	assert db.get_affected_rows_count() == 1

	db.exec_params('insert into users VALUES (?,?)', 100, 'Peter3') or {
		println(err)
		assert false
		return
	}

	db.exec('select * from users where name="Peter3333" and id=100') or {
		println(err)
		assert false
	}

	db.exec("delete from users where name='qqqq'") or {
		println(err)
		assert false
	}
	assert db.get_affected_rows_count() == 0

	db.exec("delete from users where name='Sam'") or {
		println(err)
		assert false
		return
	}
	assert db.get_affected_rows_count() == 1
}

fn test_can_access_sqlite_result_consts() {
	assert sqlite.sqlite_ok == 0
	assert sqlite.sqlite_error == 1
	// assert sqlite.misuse == 21
	assert sqlite.sqlite_row == 100
	assert sqlite.sqlite_done == 101
}

fn test_alias_db() {
	create_host(sqlite.connect(':memory:')!)!
	assert true
}
