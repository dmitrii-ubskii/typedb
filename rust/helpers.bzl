#
# Copyright (C) 2022 Vaticle
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

load("@rules_pkg//:providers.bzl", "PackageVariablesInfo")

def create_empty_dir(name, dir):
    native.genrule(
        name = name,
        cmd = "mkdir $(@D)/" + dir,
        outs = [dir]
    )

def _package_variables_info(ctx):
    values = {
        "target_os": ctx.attr.target_os,
        "target_cpu": ctx.attr.target_cpu,
        "version": ctx.var.get('version', '0.0.0')
    }
    return PackageVariablesInfo(values = values)

package_variables_info = rule(
    implementation = _package_variables_info,
    attrs = {
        "target_os": attr.string(mandatory = True),
        "target_cpu": attr.string(mandatory = True)
    }
)
