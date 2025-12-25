# Lambda Transformer


- **Purpose**: Calls an external cloud function to process data before storing it in the target database.
- **Configuration**:
    - `Options`:
        - `CloudFunction`: The name of the external cloud function.
        - `CloudFunctionsBaseURL`: The base URL for the function service (e.g., AWS Lambda, Google Cloud Functions).
        - `InvocationTimeout`: Maximum execution time for the function.
        - `NumberOfRetries`: Number of retries in case of failure.
        - `BufferSize`: Size of the buffer before sending data.
        - `BufferFlushInterval`: Interval for flushing the buffer.
        - `Headers`: Any additional headers required for API calls.
    - `TableID`: Defines the target table for transformation.
- **Example**:
  ```yaml
  - lambda:
      Options:
        CloudFunction: test_func
        CloudFunctionsBaseURL: aws_url
        InvocationTimeout: 1e+10
        NumberOfRetries: 3
        BufferSize: 1.048576e+06
        BufferFlushInterval: 1e+09
        Headers: null
      TableID:
        Name: test
        Namespace: public
    transformerId: ""
  ```
