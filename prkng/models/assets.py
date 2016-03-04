from prkng.database import db
from prkng.utils import random_string

from flask import current_app
from boto.s3.connection import S3Connection


class Images(object):
    """
    This class handles report and avatar image management, including uploading the images to Amazon S3.
    """

    @staticmethod
    def generate_s3_url(image_type, file_name):
        """
        Generate an S3 submission URL.
        The URL is valid for 24h, with which the user can upload an avatar or a report image.

        :param image_type: either 'report' or 'avatar' (str)
        :param file_name: the name of the image as uploaded, incl. extension (str)
        :returns: dict with keys `request_url` and `access_url`. Upload to the request URL, access the image from the access URL.
        """
        file_name = random_string(16) + "." + file_name.rsplit(".")[1]

        c = S3Connection(current_app.config["AWS_ACCESS_KEY"],
            current_app.config["AWS_SECRET_KEY"])
        url = c.generate_url(86400, "PUT", current_app.config["AWS_S3_BUCKET"],
            image_type+"/"+file_name, headers={"x-amz-acl": "public-read",
                "Content-Type": "image/jpeg"})

        return {"request_url": url, "access_url": url.split("?")[0]}
