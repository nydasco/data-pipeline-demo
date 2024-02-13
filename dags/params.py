class Params(object):
    storage_options = {
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": "minio123",
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT_URL": "http://10.5.0.5:9000",
        "AWS_ALLOW_HTTP": "TRUE",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "TRUE",
    }

    api_key = "86HSVB5OHEPVZB9J"

    rates = [
                {"AUD": "USD"}, 
                {"AUD": "CAD"},
                {"AUD": "GBP"},
                {"AUD": "EUR"},
                {"AUD": "NZD"},
                {"USD": "AUD"},
                {"USD": "CAD"},
                {"USD": "GBP"},
                {"USD": "EUR"},
                {"USD": "NZD"},
                {"CAD": "USD"},
                {"CAD": "AUD"},
                {"CAD": "GBP"},
                {"CAD": "EUR"},
                {"CAD": "NZD"},
                {"GBP": "USD"},
                {"GBP": "AUD"},
                {"GBP": "CAD"},
                {"GBP": "EUR"},
                {"GBP": "NZD"}
            ]