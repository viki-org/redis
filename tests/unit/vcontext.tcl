start_server {tags {"vcontext"}} {
  proc setup_data {} {
    r del x:g1 x:g2 x:us x:ca x:mx cap incl excl
    r sadd x:g1 a   c      f   h  i    k l m n o p
    r sadd x:g2     c          h
    r sadd x:us a        e   g h     j k l   n
    r sadd x:ca   b   d  e        i  j   l     o p
    r sadd x:mx a
    r sadd cap  a            g       j
  }

  test "wrong number of parameters" {
    set err "ERR wrong number of arguments for 'vcontext' command"
    assert_error $err {r vcontext }
    assert_error $err {r vcontext 1 }
    assert_error $err {r vcontext 0 0}
    assert_error $err {r vcontext 0 1}
    assert_error $err {r vcontext 0 0 1}
    assert_error $err {r vcontext 0 0 0 1}
  }

  test "syntax error for wrong number of filters and indexes" {
    set err "ERR syntax error"
    assert_error $err {r vcontext 0 0 1 abc 1}
    assert_error $err {r vcontext 0 0 0 1 abc}
    assert_error $err {r vcontext 0 0 1 abc 1 abc}
  }

  test "inclusion list is not a zset" {
    r set incl under
    set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vcontext incl 0 1 abc 1 x:us cap}
  }

  test "exclusion list is not a zset" {
    r set excl under
    set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vcontext 0 excl 1 abc 1 x:us cap}
  }

  test "nothing for an invalid set" {
    setup_data
    assert_equal {} [r vcontext 0 0 1 abc 1 x:us cap]
  }

  test "nothing for an invalid index" {
    setup_data
    assert_equal {} [r vcontext 0 0 1 x:g1 1 x9001 cap]
  }

  test "returns valid sets with a single filter" {
    setup_data
    assert_equal {x:us x:ca} [r vcontext 0 0 1 x:g1 3 x:us x:ca x:mx cap]
  }

  test "ignores invalid index" {
    setup_data
    assert_equal {x:us x:ca} [r vcontext 0 0 1 x:g1 4 x:us x:ca x:mx x:sg cap]
  }

  test "returns valid sets with a multiple filters" {
    setup_data
    assert_equal {x:us} [r vcontext 0 0 2 x:g1 x:g2 3 x:us x:ca x:mx cap]
  }

  test "returns valid set when no filter is given" {
    setup_data
    assert_equal {x:us x:ca} [r vcontext 0 0 0 3 x:us x:ca x:mx cap]
  }
}
