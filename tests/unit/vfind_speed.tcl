start_server {tags {"vfind_speed"}} {

  proc setup_data {} {
    puts "Setting up common data"

    for {set i 1} {$i<=100000} {incr i} {
      r zadd zset $i n$i
    }
          
    for {set i 1} {$i<=100000} {incr i} {
      r hset r:n$i details s_n$i
    }

    puts "Done setting up common data"
  }

  proc basics {encoding} {
    if {$encoding == "ziplist"} {
        r config set zset-max-ziplist-entries 128
        r config set zset-max-ziplist-value 64
    } elseif {$encoding == "skiplist"} {
        r config set zset-max-ziplist-entries 0
        r config set zset-max-ziplist-value 0
    } else {
        puts "Unknown sorted set encoding"
        exit
    }

    test "speed of vfindWithFilters - $encoding" {
      r del filter1 filter2 cap incl excl

      puts "Setting up data for specific test"
          
      for {set i 1} {$i<=1800} {incr i} {
        r sadd filter1 n$i
      }
          
      for {set i 1} {$i<=1800} {incr i} {
        r sadd filter2 n$i
      }
          
      for {set i 1} {$i<=199} {incr i} {
        r sadd cap n$i
      }

      puts "Begin to query"

      set t [time {
          r vfind zset cap 0 1000 25 2000 asc incl excl yes details 2 filter1 filter2
      } 30]
      
      puts $t
      assert_equal {} [r get nonexist]
    }

    test "speed of vfindZWithFilters - $encoding" {
      r del filter1 filter2 cap incl excl

      puts "Setting up data for specific test"
          
      for {set i 1} {$i<=2500} {incr i} {
        r sadd filter1 n$i
      }
          
      for {set i 1} {$i<=2500} {incr i} {
        r sadd filter2 n$i
      }
          
      for {set i 1} {$i<=199} {incr i} {
        r sadd cap n$i
      }

      puts "Begin to query"

      set t [time {
          r vfind zset cap 0 1000 25 2000 asc incl excl yes details 2 filter1 filter2
      } 30]
      
      puts $t
      assert_equal {} [r get nonexist]
    }
  }

  setup_data
  basics ziplist
  basics skiplist
}
