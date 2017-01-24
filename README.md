# DatabaseSlice

## Example Usage
Provide database information and credentials in *config.yml*.

```ruby
db_slice = DatabaseSlice.new
db_slice.pull(User, [14, 876, 943, 1069, 2347])
db_slice.insert_and_sanitize
```

## Public Methods

### initialize(*skips = []*)
Instantiates a DatabaseSlice that will copy all models except those of a class contained in *skips*.

### pull(*model, model_ids*)
Copies all records of class *model* with a primary key contained in *model_ids*, all **belongs\_to**, **has\_many**, and **has\_one** associations, and all associations' associations, recursively.

### insert\_and_sanitize(*batch_size* = 50)
Inserts all copied records into target DB in batches of *batch_size* per SQL statement.