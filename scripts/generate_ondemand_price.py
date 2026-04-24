import boto3
import json
from datetime import datetime


def get_ondemand_hourly_usd(
    instance_type: str, region_code: str = "us-east-1"
) -> float | None:
    pricing = boto3.client("pricing", region_name="us-east-1")
    # Pricing API filters on human-readable location, not region code
    region_name_map = {
        "us-east-1": "US East (N. Virginia)",
        "us-east-2": "US East (Ohio)",
        "us-west-1": "US West (N. California)",
        "us-west-2": "US West (Oregon)",
    }
    location = region_name_map[region_code]

    resp = pricing.get_products(
        ServiceCode="AmazonEC2",
        Filters=[
            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
            {"Type": "TERM_MATCH", "Field": "location", "Value": location},
            {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
            {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
            {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
        ],
        MaxResults=100,
    )

    for price_str in resp["PriceList"]:
        item = json.loads(price_str)

        terms = item.get("terms", {}).get("OnDemand", {})
        for _, term in terms.items():
            price_dimensions = term.get("priceDimensions", {})
            for _, dim in price_dimensions.items():
                unit = dim.get("unit")
                price = dim.get("pricePerUnit", {}).get("USD")
                if unit == "Hrs" and price is not None:
                    return float(price)

    return None


# Example: either hardcode, or derive from a query result / existing dataframe
instance_types = ["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"]

prices = [
    {
        "instance_type": it,
        "price_per_hour": get_ondemand_hourly_usd(it, "us-east-1"),
    }
    for it in instance_types
]
output_name = (
    f"output/public/{datetime.now().year}-{datetime.now().strftime('%m')}-pricing.json"
)
with open(output_name, "w") as f:
    json.dump(prices, f)
