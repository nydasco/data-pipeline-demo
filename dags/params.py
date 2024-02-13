class Params(object):
    storage_options = {
        "AWS_ACCESS_KEY_ID": "admin",
        "AWS_SECRET_ACCESS_KEY": "some_password",
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT_URL": "http://10.5.0.5:9000",
        "AWS_ALLOW_HTTP": "TRUE",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "TRUE",
    }

    api_key = "TXCEV1NK9XB4C2LS"

    rates = [
                {"AUD": "USD"}, 
                {"AUD": "CAD"},
                {"AUD": "GBP"}
            ]