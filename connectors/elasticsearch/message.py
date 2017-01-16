
class ElasticSearchWrite:
    @classmethod
    def object_created(cls, obj):
        return {'data': obj,
                'status_code': 3006,
                'reason': 'object created successfully'}

    @classmethod
    def object_updated(cls, obj):
        return {'data': obj,
                'status_code': 3007,
                'reason': 'object updated successfully'}

class ElasticSearchRead:
    @classmethod
    def object_found(cls, obj):
        return {'data': obj,
                'status_code': 3008,
                'reason': 'object found'}

    @classmethod
    def objects_found(cls, obj):
        return {'data': obj,
                'status_code': 3009,
                'reason': 'objects found'}
