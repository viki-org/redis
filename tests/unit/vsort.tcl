start_server {tags {"vsort"}} {
  proc setup_data {} {
    r del cap anti
  }

  test "wrong number of parameters" {
    set err "ERR wrong number of arguments for 'vsort' command"
    assert_error $err {r vsort }
    assert_error $err {r vsort cap }
    assert_error $err {r vsort cap anti}
    assert_error $err {r vsort cap anti 5}
    assert_error $err {r vsort cap anti 5 us}
  }

  test "wrong type for count" {
    set err "ERR value is not an integer or out of range"
    assert_error $err {r vsort cap anti abc us 100v}
  }

  test "wrong type for country" {
    set err "ERR value is out of range"
    assert_error $err {r vsort cap anti 5 abc 100v}
  }

  test "cap is not a set" {
    r set cap over9000
    set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vsort cap anti 5 gp 100v}
  }

  test "anti is not a set" {
    r set anti under
    set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vsort cap anti 5 gp 100v}
  }

  # test "nothing" {
  #   setup_data
  #   r hset r:100v:views:recent gp 23
  #   r hset r:200v:views:recent gp 43
  #   assert_equal {0} [r vsort cap anti 5 gp 100v 200v]
  # }
}
