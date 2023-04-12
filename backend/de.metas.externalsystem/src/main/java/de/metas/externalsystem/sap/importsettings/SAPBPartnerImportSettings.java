/*
 * #%L
 * de.metas.externalsystem
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

package de.metas.externalsystem.sap.importsettings;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import javax.annotation.Nullable;

@Value
public class SAPBPartnerImportSettings
{
	@NonNull
	final int seqNo;
	@NonNull
	String partnerCodePattern;

	boolean isSingleBPartner;

	@Nullable
	String bpGroupName;

	@Builder
	public SAPBPartnerImportSettings(
			final int seqNo,
			@NonNull final String partnerCodePattern,
			final boolean isSingleBPartner,
			@Nullable final String bpGroupName)
	{
		this.seqNo = seqNo;
		this.partnerCodePattern = partnerCodePattern;
		this.isSingleBPartner = isSingleBPartner;
		this.bpGroupName = bpGroupName;
	}
}
