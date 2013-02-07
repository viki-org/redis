start_server {tags {"vsort"}} {
  proc setup_data {} {
    r del cap anti
    r zadd l:-2l:videos:us 20 300v 9 500v 100 600v
    r zadd l:-2l:containers:us 10 100c 30 200c 5 400c
    r hset r:100c meta r100
    r hset r:200c meta r200
    r hset r:300v meta r300
    r hset r:400c meta r400
    r hset r:500v meta r500
    r hset r:600v meta r600
  }

  test "wrong number of parameters" {
    set err "ERR wrong number of arguments for 'vsort' command"
    assert_error $err {r vsort }
    assert_error $err {r vsort cap }
    assert_error $err {r vsort cap anti}
    assert_error $err {r vsort cap anti 5}
    assert_error $err {r vsort cap anti 5 6}
  }

  test "wrong type for count" {
    set err "ERR value is not an integer or out of range"
    assert_error $err {r vsort cap anti abc l:-2l:videos:us l:-2l:containers:us 100v}
  }

  test "cap is not a set" {
    r set cap over9000
    set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vsort cap anti 5 l:-2l:videos:us l:-2l:containers:us 100v}
  }

  test "anti is not a set" {
    r set anti under
    set err "ERR Operation against a key holding the wrong kind of value"
    assert_error $err {r vsort cap anti 5 l:-2l:videos:us l:-2l:containers:us 100v}
  }

  test "returns less results than requested if we don't have enough matches" {
    setup_data
    assert_equal {r100 r200} [r vsort cap anti 5 l:-2l:videos:us l:-2l:containers:us 100c 200c]
  }

  test "returns results when the count matches the exact number requested" {
    setup_data
    assert_equal {r100 r200 r300} [r vsort cap anti 3 l:-2l:videos:us l:-2l:containers:us 100c 200c 300v]
  }

  test "returns the results limited by most viewed" {
    setup_data
    assert_equal {r600 r200 r300} [r vsort cap anti 3 l:-2l:videos:us l:-2l:containers:us 100c 200c 300v 400c 500v 600v]
  }

  test "applies holdbacks" {
    setup_data
    r sadd cap 200c 300v
    assert_equal {r100 r600 r500} [r vsort cap anti 3 l:-2l:videos:us l:-2l:containers:us 100c 200c 300v 400c 500v 600v]
  }

  test "applies anti_cap" {
    setup_data
    r sadd cap 200c 300v
    r sadd anti 300v
    assert_equal {r100 r300 r600} [r vsort cap anti 3 l:-2l:videos:us l:-2l:containers:us 100c 200c 300v 400c 500v 600v]
  }
}
