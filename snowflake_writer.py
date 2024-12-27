import logging
import global_properties as prop


class SnowflakeWriter:
    def __init__(self, config_path="config/global_properties.config"):
        self.logger = logging.getLogger("SnowflakeWriter")
        # Load Snowflake configurations

    def write(self, df):
        try:
            df.write \
                .format("snowflake") \
                .options(**prop.sf_options)\
                .option("dbtable", "RELTIO_ENTITY_EVENTS") \
                .mode("append") \
                .save()
            self.logger.info("Data successfully written to Snowflake.")
        except Exception as e:
            self.logger.error(f"Error writing to Snowflake: {e}")