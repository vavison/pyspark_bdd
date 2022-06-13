Feature: per-store loyalty report

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