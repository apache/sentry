Reference: https://www.apache.org/dev/licensing-howto.html

LICENSE file communicates the licensing of all content in an Apache product distribution.
It always contains the text of the Apache License and the license information of all the
permissively licensed dependencies that are distributed  by that Apache product.

HERE IS WHAT WE HAVE IN PLACE NOW.

we have an automated license generation process in place which creates a license file adding
pointers to the dependency's license within the distribution. When sentry is build,
license-maven-plugin which kicks in package stage of mvn and gathers the license information
of all the jars that are distributed. Here is the license information that we currently have,
License file is generated in sentry-dist which will have the license information from top level
LICENSE.txt and license details of all the third party jars distributed and the path to their
license files.

Currently sentry is distributing jars with licenses listed below
    1. BSD License
    2. 2-Clause BSD License
    3. 3-Clause BSD License
    4. CDDL 1.0
    5. CDDL 1.1
    6. Eclipse Public License
    7. MIT License
    8. Mozilla Public License

License files for all these licenses are already made available in sentry-dist/src/main/resources/licences/


WHAT RELEASE MANAGERS ARE RESPONSIBLE FOR WHILE MAKING A RELEASE?

1. Make sure that the new jar that is been added belongs to one of the licenses that are made
   available in sentry-dist/src/main/resources/licences/
2. When a jar that is added doesn't belong to any of the licences we have in our distribution, LICENSE.txt file
   generated in sentry-dist will create a pointer to the new license file which doesn't exist.
   It is the responsibility of the release manager to add the appropriate file in the sentry-dist/src/main/resources/licences/
   and make the pointer valid.

   Here is an example: Let's say there are two jars added in a release
    group-id: group1
    artifact id: discover
    version: 1.1
    License: ABC

    group-id: group2
    artifact id: adventure
    version: 1.5
    License: MIT License


    License file that is generated in sentry-dist would have some thing like this when sentry is built.

    The binary distribution of this product bundles these dependencies under the a
    "ABC" license
    For details, see the associated license in
    src/main/resource/licenses/ABC.txt:
    * group1:discover:1.1 -> URL

    "ABC" license
    For details, see the associated license in
    src/main/resource/licenses/MIT_License.txt:
    * group2:adventure:1.1 -> URL

    In this example, artifact "discover" has a license ABC which is not there in distribution till date so there is no
    file src/main/resource/licenses/ABC.txt. Release manager should create it and add appropriate license information.
    This new file should be committed.
