# pyspark_bdd

Simple examples of using pytest-bdd to test scenarios in pyspark.

## Running the tests in this repo

You will need to [install poetry](https://python-poetry.org/docs/#installation) and run `poetry install` to initialise the virtual environment.

Then you can run `poetry run pytest --gherkin-terminal-reporter --verbose` and view the output of the BDD tests.

## Why BDD for PySpark?

> “The hardest single part of building a software system is deciding precisely what to build.”<br> \- _Fred Brooks, The mythical man-month_

BDD helps to bridge the communication gap within your agile data team by bringing together technical and non-technical team members to collaborate on defining example-based test specifications which can be written and understood by anyone.

e.g.

```gherkin
  Scenario: Basic aggregations at single store
    Given the following transactions:
      | transaction_id: string | store_name: string | transaction_type: string | points_delta: int | date: date |
      | 1                      | Store A            | EARN                     | 20                | 2022-08-09 |
      | 2                      | Store A            | BURN                     | -30               | 2022-08-09 |
      | 3                      | Store A            | EARN                     | 25                | 2022-08-09 |
      | 4                      | Store A            | BURN                     | -10               | 2022-08-10 |
    When we generate the per-store loyalty report
    Then the report output should be:
      | store_name: string | date: date | points_earned: bigint | points_burned: bigint |
      | Store A            | 2022-08-09 | 45                    | -30                   |
      | Store A            | 2022-08-10 | 0                     | -10                   |

```

This is particularly useful when developing data transformations as it is often difficult for a non-technical stakeholder to verify the correctness of a big data artifact just by looking at it. If aggregations are involved, any edge cases will get swallowed up and we can't trace exactly how they were handled. All that can really be confirmed with that method is that the numbers look 'about right'.

By defining BDD scenarios for all our edge cases, we can ensure that the team has a clear, shared view as to how they should all be handled, and that the application is handling them as desired.
