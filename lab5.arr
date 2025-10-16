use context dcic2024

include csv

# Task une

#|
   Plan for data cleaning
   
Input this stuff:
   
- flights_sample53.csv with columns: rownames, dep_time, sched_dep_time, 
  dep_delay, arr_time, sched_arr_time, arr_delay, carrier, flight, tailnum, 
  origin, dest, air_time, distance, hour, minute, time_hour

Output this stuff:
   
- Cleaned table with:
   
  No missing tailnum values (replaced with "UNKNOWN")
  Non-negative delay values
  Normalized carrier codes (trimmed, uppercase)
  Duplicate identification via dedup_key column

- Grouped table showing duplicate counts

   Steps (hopefully):
   
1. Load the CSV file
   
2. Convert numeric columns from strings to numbers using the transform-column thingy
   
3. Replace missing "tailnum" values ("") with "UNKNOWN"

   4. Clean dep_delay: replace negative values with 0

   5. Clean arr_delay: replace negative values with 0 

6. Create dedup_key column by:
   
   Converting flight to string
   Normalizing dep_time to "HH:MM" format
   Concatenating all three with hyphens

7. Use count() to identify duplicates by dedup_key

   8. Order by count descending to see duplicates first??

   quick notes:
   
   - Use transform-column first
   - Then use build-column
   - Write helper functions: trim() and format-time()
   - Use lambda expressions for inline transformations
   
|#



# Task deux: Oh no! missing data! clean, clean!

# Helper function: Trim spaces

fun trim(s :: String) -> String:
  doc: "Remove spaces from the given string."
  n = string-length(s)
  if n == 0:
    ""
  else:
    string-replace(s, " ", "")
  end
where:
  trim("  UA  ") is "UA"
  trim("AA") is "AA"
  trim("") is ""
end

# Helper function; format time as HH:MM

fun format-time(time-num :: Number) -> String:
  doc: "Converts a numeric time (like, 517, for example) to HH:MM format (e.g., '05:17')"
  hours = num-floor(time-num / 100)
  minutes = num-modulo(time-num, 100)
  
  # Add 0's if needed
  hours-str = if hours < 10:
    "0" + num-to-string(hours)
  else:
    num-to-string(hours)
  end
  
  minutes-str = if minutes < 10:
    "0" + num-to-string(minutes)
  else:
    num-to-string(minutes)
  end
  
  hours-str + ":" + minutes-str
where:
  format-time(517) is "05:17"
  format-time(1200) is "12:00"
  format-time(5) is "00:05"
  format-time(2359) is "23:59"
end

# Load the flights_csv table
flights-53-raw = load-table: rownames, dep_time, sched_dep_time, dep_delay, arr_time, sched_arr_time, arr_delay, carrier, flight, tailnum, origin, dest, air_time, distance, hour, minute, time_hour
  source: csv-table-file("flights.csv", default-options)
end

# Convert string columns to numbers
flights-53-converted = flights-53-raw
  .transform-column("rownames", lam(v): string-to-number(v).value end)
  .transform-column("dep_time", lam(v): string-to-number(v).value end)
  .transform-column("sched_dep_time", lam(v): string-to-number(v).value end)
  .transform-column("dep_delay", lam(v): string-to-number(v).value end)
  .transform-column("arr_time", lam(v): string-to-number(v).value end)
  .transform-column("sched_arr_time", lam(v): string-to-number(v).value end)
  .transform-column("arr_delay", lam(v): string-to-number(v).value end)
  .transform-column("flight", lam(v): string-to-number(v).value end)
  .transform-column("air_time", lam(v): string-to-number(v).value end)
  .transform-column("distance", lam(v): string-to-number(v).value end)
  .transform-column("hour", lam(v): string-to-number(v).value end)
  .transform-column("minute", lam(v): string-to-number(v).value end)

# Replace missing tailnum values with "UNKNOWN"
flights-53-tailnum = transform-column(flights-53-converted, "tailnum",
  lam(tn):
    if tn == "":
      "UNKNOWN"
    else:
      tn
    end
  end)

# Replace negative dep_delay with 0
flights-53-dep = transform-column(flights-53-tailnum, "dep_delay",
  lam(delay):
    if delay < 0:
      0
    else:
      delay
    end
  end)

# Replace negative arr_delay with 0
flights-53-clean = transform-column(flights-53-dep, "arr_delay",
  lam(delay):
    if delay < 0:
      0
    else:
      delay
    end
  end)

# Create dedup_key column
flights-53-dedup = build-column(flights-53-clean, "dedup_key",
  lam(r):
    
    # Normalize flight number
    flight-str = num-to-string(r["flight"])
    
    # Normalize carrier (trim and uppercase)
    carrier-norm = string-to-upper(trim(r["carrier"]))
    
    # Normalize dep_time to HH:MM format
    time-str = format-time(r["dep_time"])
    
    # Concatenate
    flight-str + "-" + carrier-norm + "-" + time-str
    
  end)

# Identify duplicates using count thingy
duplicate-counts = count(flights-53-dedup, "dedup_key")
duplicate-counts-ordered = order-by(duplicate-counts, "count", false)

"Task 2 Results:"
"Cleaned flights table with dedup_key:"
flights-53-dedup

"Duplicate counts (ordered by frequency):"
duplicate-counts-ordered


# Task trois: Normalize category values

# Create airline column mapping carrier codes to full names
fun carrier-to-airline(carrier :: String) -> String:
  doc: "Maps carrier code to full airline name"
  ask:
    | carrier == "UA" then: "United Airlines"
    | carrier == "AA" then: "American Airlines"
    | carrier == "B6" then: "JetBlue"
    | carrier == "DL" then: "Delta Air Lines"
    | carrier == "EV" then: "ExpressJet"
    | carrier == "WN" then: "Southwest Airlines"
    | carrier == "OO" then: "SkyWest Airlines"
    | otherwise: "Other"
  end
where:
  carrier-to-airline("UA") is "United Airlines"
  carrier-to-airline("AA") is "American Airlines"
  carrier-to-airline("XYZ") is "Other"
end

flights-53-airline = build-column(flights-53-dedup, "airline",
  lam(r):
    carrier-to-airline(r["carrier"])
  end)

# Filter out outliers: distance > 5000 or air_time < 500 is unrealistic (not to be rude)
flights-53-final = filter-with(flights-53-airline,
  lam(r):
    (r["distance"] <= 5000) and (r["air_time"] >= 500)
  end)

"Task 3 Results:"
"Flights with airline names and outliers removed:"
flights-53-final


# Task quatre: Visualization & for the loops

# Visualisation onee: Frequency bar chart of flights (airline)
"Visualisation 1: Flights by Airline"
freq-bar-chart(flights-53-final, "airline")

# Visualisation two: Histogram of flight (distances)
"Visualisation 2: Flight Distances Histogram"
histogram(flights-53-final, "distance", 10)

# Visualisation three: Scatter plot of air_time vs distance
"Visualisation 3: Air Time vs Distance"
scatter-plot(flights-53-final, "distance", "air_time")



distance-list = flights-53-final.all-rows().map(lam(r): r["distance"] end)

"Distance list:"
distance-list


# Stats using for each loop
var total-distance = 0
var max-distance = 0

for each(dist from distance-list) block:

  total-distance := total-distance + dist
  
  # update max
  when dist > max-distance:
    max-distance := dist
  end
end

# Calculate avg
avg-distance = total-distance / distance-list.length()

# SHOW ME NOWWW
print("Task 4 Results - Distance Statistics:")
print("Total distance flown: " + num-to-string(total-distance))
print("Average distance: " + num-to-string(avg-distance))
print("Maximum distance: " + num-to-string(max-distance))