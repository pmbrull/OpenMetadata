#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
OpenMetadata Airflow Lineage Backend security providers config
"""
import ast

from airflow.configuration import conf

from airflow_provider_openmetadata.lineage.config.commons import LINEAGE
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    AuthProvider,
)
from metadata.generated.schema.security.client.auth0SSOClientConfig import (
    Auth0SSOClientConfig,
)
from metadata.generated.schema.security.client.azureSSOClientConfig import (
    AzureSSOClientConfig,
)
from metadata.generated.schema.security.client.customOidcSSOClientConfig import (
    CustomOIDCSSOClientConfig,
)
from metadata.generated.schema.security.client.googleSSOClientConfig import (
    GoogleSSOClientConfig,
)
from metadata.generated.schema.security.client.oktaSSOClientConfig import (
    OktaSSOClientConfig,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.security.credentials.gcsCredentials import (
    GCSCredentials,
    GCSCredentialsPath,
    GCSValues,
)
from metadata.utils.credentials import validate_private_key
from metadata.utils.dispatch import enum_register

provider_config_registry = enum_register()


class InvalidAirflowProviderException(Exception):
    """
    Raised when we cannot find the provider
    in Airflow config
    """


@provider_config_registry.add(AuthProvider.google.value)
def load_google_auth() -> GoogleSSOClientConfig:
    """
    Load config for Google Auth
    """
    audience = conf.get(
        LINEAGE, "audience", fallback="https://www.googleapis.com/oauth2/v4/token"
    )
    secret_key = conf.get(LINEAGE, "secret_key", fallback=None)
    if secret_key:
        return GoogleSSOClientConfig(
            credentials=GCSCredentials(gcsConfig=secret_key),
            audience=audience,
        )

    private_key = ast.literal_eval(conf.get(LINEAGE, "private_key"))
    validate_private_key(private_key)

    credentials = GCSCredentials(
        gcsConfig=GCSValues(
            type=conf.get(LINEAGE, "type"),
            projectId=conf.get(LINEAGE, "project_id"),
            privateKeyId=conf.get(LINEAGE, "private_key_id"),
            privateKey=private_key,
            clientEmail=conf.get(LINEAGE, "client_email"),
            clientId=conf.get(LINEAGE, "client_id"),
            authUri=conf.get(LINEAGE, "auth_uri"),
            tokenUri=conf.get(LINEAGE, "token_uri"),
            authProviderX509CertUrl=conf.get(LINEAGE, "auth_provider_x509_cert_url"),
            clientX509CertUrl=conf.get(LINEAGE, "client_x509_cert_url"),
        )
    )

    return GoogleSSOClientConfig(
        credentials=credentials,
        audience=audience,
    )


@provider_config_registry.add(AuthProvider.okta.value)
def load_okta_auth() -> OktaSSOClientConfig:
    """
    Load config for Google Auth
    """
    return OktaSSOClientConfig(
        clientId=conf.get(LINEAGE, "client_id"),
        orgURL=conf.get(LINEAGE, "org_url"),
        privateKey=conf.get(LINEAGE, "private_key"),
        email=conf.get(LINEAGE, "email"),
        scopes=conf.get(LINEAGE, "scopes", fallback=[]),
    )


@provider_config_registry.add(AuthProvider.auth0.value)
def load_auth0_auth() -> Auth0SSOClientConfig:
    """
    Load config for Google Auth
    """
    return Auth0SSOClientConfig(
        clientId=conf.get(LINEAGE, "client_id"),
        secretKey=conf.get(LINEAGE, "secret_key"),
        domain=conf.get(LINEAGE, "domain"),
    )


@provider_config_registry.add(AuthProvider.azure.value)
def load_azure_auth() -> AzureSSOClientConfig:
    """
    Load config for Azure Auth
    """
    return AzureSSOClientConfig(
        clientSecret=conf.get(LINEAGE, "client_secret"),
        authority=conf.get(LINEAGE, "authority"),
        clientId=conf.get(LINEAGE, "client_id"),
        scopes=conf.get(LINEAGE, "scopes", fallback=[]),
    )


@provider_config_registry.add(AuthProvider.custom_oidc.value)
def load_custom_oidc_auth() -> CustomOIDCSSOClientConfig:
    """
    Load config for Azure Auth
    """
    return CustomOIDCSSOClientConfig(
        clientId=conf.get(LINEAGE, "client_id"),
        secretKey=conf.get(LINEAGE, "secret_key"),
        tokenEndpoint=conf.get(LINEAGE, "token_endpoint"),
    )


@provider_config_registry.add(AuthProvider.openmetadata.value)
def load_om_auth() -> OpenMetadataJWTClientConfig:
    """
    Load config for Azure Auth
    """
    return OpenMetadataJWTClientConfig(jwtToken=conf.get(LINEAGE, "jwt_token"))
