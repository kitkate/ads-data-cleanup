from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import StructType


def cleanup_colors(df):
    return df.withColumn(
        "attributes",
        col("attributes").withField(
            "exteriorColor", regexp_replace("attributes.exteriorColor", "_", "")
        ),
    )

def flatten_fields(schema, prefix=None):
    fields = []
    for field in schema.fields:
        original_name = prefix + "." + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, StructType):
            fields += flatten_fields(dtype, prefix=original_name)
        else:
            new_name = original_name.replace(".", "__").lower()
            fields.append(col(original_name).cast("string").alias(new_name))

    return fields

def flatten(df):
    return df.select(flatten_fields(df.schema))



