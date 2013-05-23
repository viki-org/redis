start_server {tags {"vcontext"}} {
  proc setup_data {} {
    r del x:g1 x:g2 x:us x:ca x:mx cap anti incl excl
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
    assert_error $err {r vcontext cap }
    assert_error $err {r vcontext cap anti}
    assert_error $err {r vcontext cap anti incl}
    assert_error $err {r vcontext cap anti incl excl}
    assert_error $err {r vcontext cap anti incl excl 0}
    assert_error $err {r vcontext cap anti incl excl 0 0}
  }

  test "cap is not a set" {
    r set cap nocap
    set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vcontext cap anti incl excl 0 1 x:us}
  }

  test "anti cap is not a set" {
    r set anti noanti
    set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vcontext cap anti incl excl 0 1 x:us}
  }

  test "inclusion list is not a zset" {
    r set incl under
    set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vcontext cap anti incl excl 0 1 x:us}
  }

  test "exclusion list is not a zset" {
    r set excl under
    set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vcontext cap anti incl excl 0 1 x:us}
  }

  test "nothing for an invalid set" {
    setup_data
    assert_equal {} [r vcontext cap anti incl excl 1 abc 1 x:us]
  }

  test "nothing for an invalid index" {
    setup_data
    assert_equal {} [r vcontext cap anti incl excl 1 x:g1 1 x9001]
  }

  test "returns valid sets with a single filter" {
    setup_data
    assert_equal {x:us x:ca} [r vcontext cap anti incl excl 1 x:g1 3 x:us x:ca x:mx]
  }

  test "ignores invalid index" {
    setup_data
    assert_equal {x:us x:ca} [r vcontext cap anti incl excl 1 x:g1 4 x:us x:ca x:mx x:sg]
  }

  test "returns valid sets with a multiple filters" {
    setup_data
    assert_equal {x:us} [r vcontext cap anti incl excl 2 x:g1 x:g2 3 x:us x:ca x:mx]
  }

  test "returns valid set when no filter is given" {
    setup_data
    assert_equal {x:us x:ca} [r vcontext cap anti incl excl 0 3 x:us x:ca x:mx]
  }

  test "applies cap" {
    setup_data
    r sadd cap b d e i j l o p b d e i j l o p
    assert_equal {x:us} [r vcontext cap anti incl excl 0 3 x:us x:ca x:mx]
  }

  test "applies anti cap" {
    setup_data
    r sadd cap b d e i j l o p b d e i j l o p
    r sadd anti e
    assert_equal {x:us x:ca} [r vcontext cap anti incl excl 0 3 x:us x:ca x:mx]
  }

  test "applies inclusion" {
    setup_data
    r sadd cap b d e i j l o p b d e i j l o p
    r zadd incl 0 e
    assert_equal {x:us x:ca} [r vcontext cap anti incl excl 0 3 x:us x:ca x:mx]
  }

  test "applies exclusion" {
    setup_data
    r sadd excl b d e i j l o p b d e i j l o p
    assert_equal {x:us} [r vcontext cap anti incl excl 0 3 x:us x:ca x:mx]
  }
}
