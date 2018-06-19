#!/usr/bin/env python3

import os
import sys
import shutil
import logging
import argparse
import logging.config

from distutils.dir_util import copy_tree

from os.path import join, dirname

try:
    from __init__ import __version__, __build__
    from generator.engine import NextflowGenerator, process_map
    from generator.inspect import NextflowInspector
    from generator.recipe import brew_recipe, available_recipes
    from generator.pipeline_parser import parse_pipeline, SanityError
    from generator.process_details import proc_collector, colored_print
    import generator.error_handling as eh
except ImportError:
    from flowcraft import __version__, __build__
    from flowcraft.generator.engine import NextflowGenerator, process_map
    from flowcraft.generator.inspect import NextflowInspector
    from flowcraft.generator.recipe import brew_recipe, available_recipes
    from flowcraft.generator.pipeline_parser import parse_pipeline, \
        SanityError
    from flowcraft.generator.process_details import proc_collector, \
        colored_print
    import flowcraft.generator.error_handling as eh

logger = logging.getLogger("main")


def get_args(args=None):

    parser = argparse.ArgumentParser(
        description="A Nextflow pipeline generator")

    subparsers = parser.add_subparsers(help="Select which mode to run",
                                       dest="main_op")

    # BUILD MODE
    build_parser = subparsers.add_parser("build",
                                         help="Build a nextflow pipeline")

    group_lists = build_parser.add_mutually_exclusive_group()

    build_parser.add_argument(
        "-t", "--tasks", type=str, dest="tasks",
        help="Space separated tasks of the pipeline")
    build_parser.add_argument(
        "-r", "--recipe", dest="recipe",
        help="Use one of the available recipes")
    build_parser.add_argument(
        "-o", dest="output_nf", help="Name of the pipeline file")
    build_parser.add_argument(
        "-n", dest="pipeline_name", default="flowcraft",
        help="Provide a name for your pipeline.")
    build_parser.add_argument(
        "--pipeline-only", dest="pipeline_only", action="store_true",
        help="Write only the pipeline files and not the templates, bin, and"
             " lib folders.")
    build_parser.add_argument(
        "-nd", "--no-dependecy", dest="no_dep", action="store_false",
        help="Do not automatically add dependencies to the pipeline.")
    build_parser.add_argument(
        "-c", "--check-pipeline", dest="check_only", action="store_const",
        const=True, help="Check only the validity of the pipeline "
                         "string and exit.")
    group_lists.add_argument(
        "-L", "--detailed-list", action="store_const", dest="detailed_list",
        const=True, help="Print a detailed description for all the "
                         "currently available processes")
    group_lists.add_argument(
        "-l", "--short-list", action="store_const", dest="short_list",
        const=True, help="Print a short list of the currently "
                         "available processes")
    build_parser.add_argument("-cr", "--check-recipe", dest="check_recipe",
                              action="store_const", const=True,
                              help="Check tasks that the recipe contain and "
                                   "their flow. This option might be useful "
                                   "if a user wants to change some components "
                                   "of a given recipe, by using the -t option.")

    # GENERAL OPTIONS
    parser.add_argument(
        "--debug", dest="debug", action="store_const", const=True,
        help="Set log to debug mode")
    parser.add_argument(
        "-v, --version", dest="version", action="store_const", const=True,
        help="Show version and exit.")

    # INSPECT MODE
    inspect_parser = subparsers.add_parser("inspect",
                                           help="Inspect the progress of a "
                                                "pipeline execution")
    inspect_parser.add_argument(
        "-i", dest="trace_file", default="pipeline_stats.txt",
        help="Specify the nextflow trace file."
    )
    inspect_parser.add_argument(
        "-r", dest="refresh_rate", default=0.02,
        help="Set the refresh frequency for the continuous inspect functions"
    )
    inspect_parser.add_argument(
        "-m", "--mode", dest="mode", default="overview",
        choices=["overview", "broadcast"],
        help="Specify the inspection run mode."
    )
    inspect_parser.add_argument(
        "-u", "--url", dest="url", default="http://192.92.149.169:80/",
        help="Specify the URL to where the data should be broadcast"
    )
    inspect_parser.add_argument(
        "--pretty", dest="pretty", action="store_const", const=True,
        help="Pretty inspection mode that removes usual reporting processes."
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    return parser.parse_args(args)


def validate_build_arguments(args):

    if args.detailed_list or args.short_list:
        return

    if not args.tasks and not args.recipe and not args.check_only \
            and not args.detailed_list and not args.short_list:
        logger.error(colored_print(
            "At least one of these options is required: -t, -r, -c, "
            "-l, -L", "red_bold"))
        sys.exit(1)

    if (args.tasks or args.recipe) and not args.check_recipe \
            and not args.output_nf:
        logger.error(colored_print(
            "Please provide the path and name of the pipeline file using the"
            " -o option.", "red_bold"))
        sys.exit(1)

    if args.output_nf:
        parsed_output_nf = (args.output_nf if args.output_nf.endswith(".nf")
                            else "{}.nf".format(args.output_nf.strip()))
        opath = parsed_output_nf
        if os.path.dirname(opath):
            parent_dir = os.path.dirname(opath)
            if not os.path.exists(parent_dir):
                logger.error(colored_print(
                    "The provided directory '{}' does not exist.".format(
                        parent_dir), "red_bold"))
                sys.exit(1)

        return parsed_output_nf


def copy_project(path):
    """

    Parameters
    ----------
    path

    Returns
    -------

    """

    # Get nextflow repo directory
    repo_dir = dirname(os.path.abspath(__file__))

    # Get target directory
    target_dir = dirname(path)

    # Copy templates
    copy_tree(join(repo_dir, "templates"), join(target_dir, "templates"))

    # Copy Helper scripts
    copy_tree(join(repo_dir, "lib"), join(target_dir, "lib"))

    # Copy bin scripts
    copy_tree(join(repo_dir, "bin"), join(target_dir, "bin"))

    # Copy default config file
    shutil.copy(join(repo_dir, "nextflow.config"),
                join(target_dir, "nextflow.config"))

    # Copy static profiles file
    shutil.copy(join(repo_dir, "profiles.config"),
                join(target_dir, "profiles.config"))


def build(args):

    welcome = [
        "========= F L O W C R A F T =========",
        "Build mode\n"
        "version: {}".format(__version__),
        "build: {}".format(__build__),
        "====================================="
    ]

    parsed_output_nf = validate_build_arguments(args)

    logger.info(colored_print("\n".join(welcome), "green_bold"))

    # If a recipe is specified, build pipeline based on the
    # appropriate recipe
    if args.recipe:
        if args.recipe == "innuendo":
            pipeline_string = brew_recipe(args, available_recipes)
        else:
            pipeline_string = available_recipes[args.recipe]
            if args.tasks:
                logger.warning(colored_print(
                    "-t parameter will be ignored for recipe: {}\n"
                        .format(args.recipe), "yellow_bold")
                )

        if args.check_recipe:
            logger.info(colored_print("Pipeline string for recipe: {}"
                                      .format(args.recipe), "purple_bold"))
            logger.info(pipeline_string)
            sys.exit(0)
    else:
        pipeline_string = args.tasks

    # used for lists print
    proc_collector(process_map, args, pipeline_string)

    logger.info(colored_print("Resulting pipeline string:\n"))
    logger.info(colored_print(pipeline_string + "\n"))

    try:
        logger.info(colored_print("Checking pipeline for errors..."))
        pipeline_list = parse_pipeline(pipeline_string)
    except SanityError as e:
        logger.error(colored_print(e.value, "red_bold"))
        sys.exit(1)
    logger.debug("Pipeline successfully parsed: {}".format(pipeline_list))

    # Exit if only the pipeline parser needs to be checked
    if args.check_only:
        sys.exit()

    nfg = NextflowGenerator(process_connections=pipeline_list,
                            nextflow_file=parsed_output_nf,
                            pipeline_name=args.pipeline_name,
                            auto_dependency=args.no_dep)

    logger.info(colored_print("Building your awesome pipeline..."))

    # building the actual pipeline nf file
    nfg.build()

    # copy template to cwd, to allow for immediate execution
    if not args.pipeline_only:
        copy_project(parsed_output_nf)

    logger.info(colored_print("DONE!", "green_bold"))


def inspect(args):

    try:
        nf_inspect = NextflowInspector(args.trace_file, args.refresh_rate,
                                       args.pretty, args.url)
    except eh.InspectionError as e:
        logger.error(colored_print(e.value, "red_bold"))
        sys.exit(1)

    if args.mode == "overview":
        nf_inspect.display_overview()

    if args.mode == "broadcast":
        nf_inspect.broadcast_status()


def main():

    args = get_args()

    if args.version:
        print(__version__)

    if args.debug:
        logger.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    else:
        logger.setLevel(logging.INFO)

        # create special formatter for info logs
        formatter = logging.Formatter('%(message)s')

    # create console handler and set level to debug
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)

    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if args.main_op == "build":
        build(args)

    if args.main_op == "inspect":
        inspect(args)


if __name__ == '__main__':

    main()
