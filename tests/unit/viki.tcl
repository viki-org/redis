start_server {tags {"viki"}} {
  proc basics {encoding} {

    test "VDIFFSTORE 1 - $encoding" {
      r del ztmpv1 stmpv2 ztmpv3
      r zadd ztmpv1 1 a 2 b 3 c
      r sadd stmpv2 b
      r vdiffstore ztmpv3 ztmpv1 stmpv2
      assert_equal {a c} [r zrange ztmpv3 0 -1]
    }

    test "VDIFFSTORE 2 - $encoding" {
      r del ztmpv1 stmpv2 ztmpv3
      r zadd ztmpv1 1 c 2 b 3 a
      r sadd stmpv2 c a b
      r vdiffstore ztmpv3 ztmpv1 stmpv2
      assert_equal {} [r zrange ztmpv3 0 -1]
    }

    test "VDIFFSTORE 3 - $encoding" {
      r del ztmpv1 stmpv2 ztmpv3
      r zadd ztmpv1 1 c
      r vdiffstore ztmpv3 ztmpv1 stmpv2
      assert_equal {c} [r zrange ztmpv3 0 -1]
    }

    test "VDIFFSTORE 4 - $encoding" {
      r del ztmpv1 stmpv2 ztmpv3
      r sadd stmpv2 c a b
      r vdiffstore ztmpv3 ztmpv1 stmpv2
      assert_equal {} [r zrange ztmpv3 0 -1]
    }

    test "VDIFFSTORE 5 - $encoding" {
      r del ztmpv1 stmpv2 ztmpv3
      r vdiffstore ztmpv3 ztmpv1 stmpv2
      assert_equal {} [r zrange ztmpv3 0 -1]
    }
  }

  basics ziplist
  basics skiplist
}