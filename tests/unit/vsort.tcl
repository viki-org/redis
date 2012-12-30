start_server {tags {"vsort"}} {
  test "wrong number of parameters" {
    set err "ERR wrong number of arguments for 'vsort' command"
    assert_error $err {r vsort }
    assert_error $err {r vsort cap }
    assert_error $err {r vsort cap anti_cap}
    assert_error $err {r vsort cap anti_cap 5}
  }

  test "wrong type for count" {
    set err "ERR value is not an integer or out of range"
    assert_error $err {r vsort cap anti_cap abc 100v 200v}
  }

  test "cap is not a set" {
    r set cap over9000
     set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vsort cap anti_cap 5 100v}
  }

  test "anti_cap is not a set" {
    r set anti_cap under
     set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vsort cap anti_cap 5 100v}
  }
}
