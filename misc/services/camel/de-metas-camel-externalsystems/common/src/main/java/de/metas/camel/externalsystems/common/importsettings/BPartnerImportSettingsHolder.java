/*
 * #%L
 * de-metas-camel-externalsystems-common
 * %%
 * Copyright (C) 2023 metas GmbH
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program. If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

package de.metas.camel.externalsystems.common.importsettings;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.metas.camel.externalsystems.common.JsonObjectMapperHolder;
import de.metas.common.externalsystem.JsonExternalSAPBPartnerImportSettings;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.Arrays;
import java.util.Map;

import static de.metas.common.externalsystem.ExternalSystemConstants.PARAM_SAP_BPARTNER_IMPORT_SETTINGS;

@Builder
@Value
public class BPartnerImportSettingsHolder
{
	ImmutableList<JsonExternalSAPBPartnerImportSettings> bPartnerImportSettings;

	@NonNull
	public static BPartnerImportSettingsHolder of(@NonNull final Map<String, String> parameters)
	{
		return BPartnerImportSettingsHolder.builder()
				.bPartnerImportSettings(deserializeBPartnerImportSettingsFromParams(parameters))
				.build();
	}

	@NonNull
	private static ImmutableList<JsonExternalSAPBPartnerImportSettings> deserializeBPartnerImportSettingsFromParams(@NonNull final Map<String, String> parameters)
	{
		final String bPartnerImportSettingsJsonString = parameters.get(PARAM_SAP_BPARTNER_IMPORT_SETTINGS);

		final ObjectMapper mapper = JsonObjectMapperHolder.sharedJsonObjectMapper();
		mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		try
		{
			return Arrays.stream(mapper.readValue(bPartnerImportSettingsJsonString, JsonExternalSAPBPartnerImportSettings[].class))
					.collect(ImmutableList.toImmutableList());
		}
		catch (final JsonProcessingException e)
		{
			throw new RuntimeException(e);
		}
	}
}
