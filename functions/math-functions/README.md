# SQRL Mathematical Functions

The `datasqrl.math` package provides a collection of advanced mathematical functions for use within the SQRL framework.
These functions are powered by the `org.apache.commons:commons-math3` library, ensuring robust and accurate mathematical computations.

## Compile and Test Manually

### Step 1: Build the Project
Ensure that all dependencies are present by running the following Maven package command:
```bash
mvn package
```

### Step 2: Run Your SQRL Script

After the build is complete, make sure to uncomment any required functions or packages in the `math.sqrl` file.
Then, run your SQRL script along with the schema using the following Docker command:
```bash
docker run -i -p 8888:8888 -p 8081:8081 --rm -v $PWD:/build datasqrl/cmd:v0.5.6 run math.sqrl schema.graphqls
```

### Step 3: Test via GraphQL

Open http://localhost:8888/graphiql/ in your browser and execute the following mutation to insert data:
```graphql
mutation {
    data(input: {
        d: 1,
        b:, 1
    }) {
        d
        b
    }
}
```

### Step 4: Retrieve the Results

To view the results, execute the following query:
```graphql
{
    math_results {
        d
        b
        cbrt
        copy_sign
        expm1
        hypot
        log1p
        next_after
        scalb
        ulp
        binomial_distribution
        exponential_distribution
        normal_distribution
        poisson_distribution
    }
}
```