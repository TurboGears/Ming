
import json
from .exc import MingConfigError
from .encryption import EncryptionConfig

try:
    from formencode import schema, validators
except ImportError:
    raise MingConfigError("Need to install FormEncode to use ``ming.validators``")


class EncryptionConfigValidator(validators.FancyValidator):
    """
    Validates and converts a dictionary of encryption settings into an EncryptionConfig object.
    The configuration object and values largely mirror the pymongo.encryption.ClientEncryption class 
    (https://pymongo.readthedocs.io/en/stable/api/pymongo/encryption.html#pymongo.encryption.ClientEncryption).

    A simple, valid configuration looks something like this

    .. code-block:: ini

        ming.maindb.uri = mongodb://localhost:27017/maindb
        ming.maindb.encryption.kms_providers.local.key = <a-96-byte-base64-encoded-string>
        ming.maindb.encryption.key_vault_namespace = encryption_test.dataKeyVault
        ming.maindb.encryption.provider_options.local.key_alt_names = datakey_test1

    For more information read up on Mongodb's Client Side Field Level Encryption (CSFLE) documentation: 
    https://pymongo.readthedocs.io/en/stable/examples/encryption.html.

    """
    accept_iterator = True

    VALID_KMS_PROVIDERS = ('local', 'aws', 'azure', 'gcp', 'kmip')
    REQUIRED_FIELDS = ('key_vault_namespace', 'provider_options', 'kms_providers')

    messages = dict(
        MissingRequiredField=(
            "Missing required encryption configuration field %(field)s."
            f" If one is present, all must be present: {REQUIRED_FIELDS}"),
        UnexpectedField=f"Unexpected encryption configuration field '%(field)s'. Valid fields are: {REQUIRED_FIELDS}",
        InvalidKMSProvider=(
            f"Invalid kms_provider(s) %(providers)s. Valid options are: {VALID_KMS_PROVIDERS}."
            " See pymongo's ClientEncryption.create_data_key for more information on valid values for kms_providers"
            " (https://pymongo.readthedocs.io/en/stable/api/pymongo/encryption.html#pymongo.encryption.ClientEncryption)."),
        InvalidKeyVaultNamespace=(
            "Invalid key_vault_namespace '%(key_vault_namespace)s'. Value must be a 'database.collection' string."
            " See pymongo's ClientEncryption for more information on valid values for key_vault_namespace"
            " (https://pymongo.readthedocs.io/en/stable/api/pymongo/encryption.html#pymongo.encryption.ClientEncryption)."),
        ProviderLocalMissingKey=(
            "kms_provider 'local' requires a 96 byte 'key' value. See pymongo's ClientEncryption for more information"
            " information on valid values for kms_providers"
            " (https://pymongo.readthedocs.io/en/stable/api/pymongo/encryption.html#pymongo.encryption.ClientEncryption)."),
        ProviderLocalMissingOptions=(
            "kms_provider 'local' requires provider_options with a 'key_alt_names' list."
            " See pymongo's ClientEncryption.create_data_key for more information"
            " (https://pymongo.readthedocs.io/en/stable/api/pymongo/encryption.html#pymongo.encryption.ClientEncryption.create_data_key)."),
    )

    def _convert_to_python(self, config: dict, state):
        if not config:
            return None

        # ensure key_alt_names is a list
        provider_options = config.get('provider_options', None) or dict()
        if provider_options:
            for provider, options in list(provider_options.items()):
                if 'key_alt_names' in options:
                    if not isinstance(options['key_alt_names'], list):
                        try:
                            config['provider_options'][provider]['key_alt_names'] = json.loads(options['key_alt_names'])
                        except json.JSONDecodeError:
                            key_alt_names = [s.strip(" ][\"'\t\r\n") for s in options['key_alt_names'].split(',') if s]
                            config['provider_options'][provider]['key_alt_names'] = key_alt_names

        return EncryptionConfig(config)

    def _validate_python(self, encryption_config: EncryptionConfig, state):
        if not encryption_config:
            # no encryption settings, so nothing to validate
            return

        def validate_inner():
            error_dict = {}

            config_dict = encryption_config._encryption_config

            required_fields = set(self.REQUIRED_FIELDS)
            provided_fields = set(config_dict.keys())
            
            extra_fields = provided_fields - required_fields
            if extra_fields:
                error_dict.update({
                    k: validators.Invalid(self.message('UnexpectedField', state, field=k), config_dict, state)
                    for k in extra_fields
                })
                # don't return early. we want to catch as many errors as possible in one pass
                # return error_dict

            valid_fields = (provided_fields & required_fields)
            if not valid_fields:
                # encryption is optional. if none of the encryption fields are present, we should allow this.
                return error_dict

            missing_fields = required_fields - provided_fields
            if missing_fields:
                error_dict.update({
                    k: validators.Invalid(self.message('MissingRequiredField', state, field=k), config_dict, state)
                    for k in missing_fields
                })
                return error_dict

            empty_fields = {k for k in self.REQUIRED_FIELDS if not config_dict.get(k, None)}
            if empty_fields == required_fields:
                # if all fields are empty, skip out here
                return error_dict
            elif empty_fields:
                error_dict.update({
                    k: validators.Invalid(self.message('MissingRequiredField', state, field=k), config_dict, state)
                    for k in empty_fields
                })
                return error_dict

            # check that all providers are valid. i.e. 'local', 'gcp', etc.
            invalid_providers = {k for k in config_dict['kms_providers'].keys() if k not in self.VALID_KMS_PROVIDERS}
            if invalid_providers:
                providers = ', '.join(invalid_providers)
                error_dict['kms_providers'] = validators.Invalid(
                    self.message('InvalidKMSProvider', state, providers=providers), config_dict, state)
                return error_dict
            
            try:
                db, coll = config_dict.get('key_vault_namespace').split('.')
            except ValueError:
                error_dict['key_vault_namespace'] = validators.Invalid(
                    self.message('InvalidKeyVaultNamespace', state, key_vault_namespace=config_dict.get('key_vault_namespace')),
                    config_dict, state)
                return error_dict
                
            # validate 'local' kms_provider settings
            if 'local' in config_dict['kms_providers']:

                if 'key' not in config_dict['kms_providers']['local']:
                    error_dict['kms_providers'] = validators.Invalid(
                        self.message('ProviderLocalMissingKey', state), config_dict, state)

                if ('local' not in config_dict['provider_options']) or ('key_alt_names' not in config_dict['provider_options']['local']):
                    error_dict['provider_options'] = validators.Invalid(
                        self.message('ProviderLocalMissingOptions', state), config_dict, state)

            return error_dict

        error_dict = validate_inner()

        if error_dict:
            raise validators.Invalid(f'Invalid Encryption Settings', encryption_config, state, error_dict=error_dict)
