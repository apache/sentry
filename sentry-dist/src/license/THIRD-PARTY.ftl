/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
<#-- To render the third-party file.
 Available context :
 - dependencyMap a collection of Map.Entry with
   key are dependencies (as a MavenProject) (from the maven project)
   values are licenses of each dependency (array of string)
 - licenseMap a collection of Map.Entry with
   key are licenses of each dependency (array of string)
   values are all dependencies using this license
-->
<#function artifactFormat p>
  <#if p.name?index_of('Unnamed') &gt; -1>
    <#return p.artifactId + " (" + p.groupId + ":" + p.artifactId + ":" + p.version + " - " + (p.url!"no url defined") + ")">
  <#else>
    <#return p.name + " (" + p.groupId + ":" + p.artifactId + ":" + p.version + " - " + (p.url!"no url defined") + ")">
  </#if>
</#function>

<#if licenseMap?size == 0>
The project has no dependencies.
<#else>
  <#list licenseMap as e>
    <#assign license = e.getKey()/>
    <#assign projects = e.getValue()/>
    <#if projects?size &gt; 0 && ("${license}"?contains("Apache") || "${license}"?contains("GPL"))>
    <#elseif projects?size &gt; 0>
The binary distribution of this product bundles these dependencies under the a
"${license}" license
For details, see the associated license in
src/main/resource/licenses/${license?replace(" ","_")?replace(".","_")?replace("/","")}.txt:
<#list projects as project>
* ${artifactFormat(project)}
</#list>

--------------------------------------------------------------------------------
    
    </#if>
  </#list>
</#if>