from django.db import models

class RunLogTable(models.Model):
    ray_id = models.CharField(max_length=100, primary_key=True)
    status = models.CharField(
        max_length=20,
        choices=[
            ('Success', 'Success'),
            ('Failed', 'Failed'),
            ('Aborted', 'Aborted'),
            ('Running', 'Running'),
        ]
    )

    def __str__(self):
        return f"{self.ray_id} - {self.status}"
