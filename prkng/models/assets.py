from prkng.database import db
from prkng.utils import random_string

from flask import current_app
from boto.s3.connection import S3Connection


class Images(object):
    @staticmethod
    def generate_s3_url(image_type, file_name):
        """
        Generate S3 submission URL valid for 24h, with which the user can upload an
        avatar or a report image.
        """
        file_name = random_string(16) + "." + file_name.rsplit(".")[1]

        c = S3Connection(current_app.config["AWS_ACCESS_KEY"],
            current_app.config["AWS_SECRET_KEY"])
        url = c.generate_url(86400, "PUT", current_app.config["AWS_S3_BUCKET"],
            image_type+"/"+file_name, headers={"x-amz-acl": "public-read",
                "Content-Type": "image/jpeg"})

        return {"request_url": url, "access_url": url.split("?")[0]}
