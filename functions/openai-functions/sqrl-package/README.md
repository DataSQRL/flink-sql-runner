# OpenAI Functions

The `datasqrl.math` package provides a collection of advanced AI and language processing functions for use within the SQRL framework.

## Functions Overview

You can use these UDFs in your SQRL scripts to perform tasks such as text completion, extraction, and embedding, leveraging the power of OpenAI models.

For each function it is required to set the `OPENAI_API_KEY` environment variable with your OpenAI API key.

| **Function Name**                   | **Description**                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|-------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **completions(String prompt, String model_name)**        | Generates a completion for the given prompt using the specified OpenAI model. For example, `completions('What is AI?', 'gpt-4o')` returns a possible response to the prompt.                                                                                                                                                                                                                                                                                         |
| **completions(String prompt, String model_name, Integer maxOutputTokens)**       | Generates a completion for the given prompt using the specified OpenAI model, with an upper limit on the number of output tokens.                                             For example, `completions('What is AI?', 'gpt-4o', 100)` returns a possible response to the prompt, limited to 100 characters.                                                                                                                                                         |
| **completions(String prompt, String model_name, Integer maxOutputTokens, Double temperature)**       | Generates a completion for the given prompt using the specified OpenAI model, with an upper limit on the number of output tokens and a specified temperature. For example, `completions('What is AI?', 'gpt-4o', 100, 0.5)` returns a possible response to the prompt, limited to 100 characters and weighted by a temperature of 0.5.                                                                                                                               |
| **completions(String prompt, String model_name, Integer maxOutputTokens, Double temperature, Double topP)**       | Generates a completion for the given prompt using the specified OpenAI model, with an upper limit on the number of output tokens, a specified temperature, and a specified top-p value. For example, `completions('What is AI?', 'gpt-4o', 100, 0.5, 0.9)` returns a possible response to the prompt, limited to 100 characters, weighted by a temperature of 0.5, and with a top-p value of 0.9.                                                                    |

| **Function Name**                   | **Description**                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|-------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **extract_json(String prompt, String model_name)**        | Extracts JSON data from the given prompt using the specified OpenAI model. For example, `extract_json('What is AI?', 'gpt-4o')` returns any relevant JSON data for the prompt.                                                                                                                                                                                                                                                                                                               |
| **extract_json(String prompt, String model_name, Double temperature)**       | Extracts JSON data from the given prompt using the specified OpenAI model and a specified temperature. For example,                                                             `extract_json('What is AI?', 'gpt-4o', 0.5)` returns any relevant JSON data for the prompt, weighted by a temperature of 0.5.                                                                                                                                                                                |
| **extract_json(String prompt, String model_name, Double temperature, Double topP)**       | Extracts JSON data from the given prompt using the specified OpenAI model, with a specified temperature and top-p value.                                                                                                                                                                                       For example, `extract_json('What is AI?', 'gpt-4o', 0.5, 0.9)` returns any relevant JSON data for the prompt, weighted by a temperature of 0.5 and with a top-p value of 0.9. |

| **Function Name**                   | **Description**                                                                                                                                                                                                                                                                                              |
|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **vector_embedd(String text, String model_name)**        | Embeds the given text into a vector using the specified OpenAI model. For example, `vector_embedd('What is AI?', 'text-embedding-ada-002')` returns a vector representation of the text.                                                                                                                     |

The functions are designed to automatically retry the API call if a failure occurs.
By default, the functions will attempt up to 3 retries. You can adjust this retry limit by setting
the `FUNCTION_MAX_RETRIES` environment variable to your desired number of retries.

## SQRL Script Example

You can use these functions in a SQRL script like this:
```sql
IMPORT schema.entry;
IMPORT datasqrl.math.*;

result_table := SELECT prompt AS pr,
    completions(prompt, 'gpt-4o') AS re,
    extract_json(prompt, 'gpt-4o') AS ex,
    CAST(vector_embedd(prompt, 'text-embedding-ada-002') AS STRING) AS em
    FROM entry;
    
-- Displaying the result
SELECT * FROM result_table;
```
This script generates completions, extracts JSON data, and embeds text vectors for each prompt in the `entry` table.
