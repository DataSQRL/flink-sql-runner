# SQRL OpenAI Functions

The `datasqrl.openai` package provides a collection of advanced functions for interacting with the OpenAI API.

## Compile and Test Manually

### Step 1: Build the Project
Ensure that all dependencies are present by running the following Maven package command:
```bash
mvn package
```

### Step 2: Run Your SQRL Script

Run your SQRL script along with the schema using the following Docker command:
```bash
docker run -i -p 8888:8888 -p 8081:8081 --rm -v $PWD:/build -e OPENAI_API_KEY="<YOUR_OPENAI_API_KEY>" datasqrl/cmd:v0.5.7 run openai.sqrl schema.graphqls
```

### Step 3: Test via GraphQL

Open http://localhost:8888/graphiql/ in your browser and execute the following mutation to insert data:
```graphql
mutation {
    data(input: {
        prompt: "Tell me a squirrel joke."
    }) {
        prompt
    }
}
```

### Step 4: Retrieve the Results

To view the results, execute the following query:
```graphql
{
    results {
        prompt
        completions_result
        extract_json_result
        vector_embedd_result
    }
}
```
