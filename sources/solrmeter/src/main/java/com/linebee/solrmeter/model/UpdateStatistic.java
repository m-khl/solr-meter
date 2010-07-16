/**
 * Copyright Linebee LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linebee.solrmeter.model;

import org.apache.solr.client.solrj.response.UpdateResponse;

import com.linebee.solrmeter.model.exception.CommitException;
import com.linebee.solrmeter.model.exception.UpdateException;

public interface UpdateStatistic {

	void onAddedDocument(UpdateResponse response);

	void onCommit(UpdateResponse response);

	void onFinishedTest();

	void onCommitError(CommitException exception);
	
	void onAddError(UpdateException exception);

}
