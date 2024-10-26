/*
 * Copyright 2013-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.valkey.connection.lettuce;

import static org.springframework.data.valkey.connection.ValkeyGeoCommands.*;
import static org.springframework.data.valkey.domain.geo.GeoReference.*;

import io.lettuce.core.*;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.ValkeyClusterNode.NodeFlag;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.valkey.connection.*;
import org.springframework.data.valkey.connection.BitFieldSubCommands.BitFieldGet;
import org.springframework.data.valkey.connection.BitFieldSubCommands.BitFieldIncrBy;
import org.springframework.data.valkey.connection.BitFieldSubCommands.BitFieldSet;
import org.springframework.data.valkey.connection.BitFieldSubCommands.BitFieldSubCommand;
import org.springframework.data.valkey.connection.Limit;
import org.springframework.data.valkey.connection.ValkeyClusterNode.Flag;
import org.springframework.data.valkey.connection.ValkeyClusterNode.LinkState;
import org.springframework.data.valkey.connection.ValkeyClusterNode.SlotRange;
import org.springframework.data.valkey.connection.ValkeyListCommands.Direction;
import org.springframework.data.valkey.connection.ValkeyListCommands.Position;
import org.springframework.data.valkey.connection.ValkeyNode.NodeType;
import org.springframework.data.valkey.connection.ValkeyStringCommands.SetOption;
import org.springframework.data.valkey.connection.SortParameters.Order;
import org.springframework.data.valkey.connection.convert.Converters;
import org.springframework.data.valkey.connection.convert.StringToValkeyClientInfoConverter;
import org.springframework.data.valkey.connection.zset.DefaultTuple;
import org.springframework.data.valkey.connection.zset.Tuple;
import org.springframework.data.valkey.core.KeyScanOptions;
import org.springframework.data.valkey.core.ScanOptions;
import org.springframework.data.valkey.core.types.Expiration;
import org.springframework.data.valkey.core.types.ValkeyClientInfo;
import org.springframework.data.valkey.domain.geo.BoundingBox;
import org.springframework.data.valkey.domain.geo.BoxShape;
import org.springframework.data.valkey.domain.geo.GeoReference;
import org.springframework.data.valkey.domain.geo.GeoShape;
import org.springframework.data.valkey.domain.geo.RadiusShape;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Lettuce type converters
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Ninad Divadkar
 * @author dengliming
 * @author Chris Bono
 * @author Vikas Garg
 * @author John Blum
 * @author Roman Osadchuk
 */
@SuppressWarnings("ConstantConditions")
public abstract class LettuceConverters extends Converters {

	public static final byte[] PLUS_BYTES;
	public static final byte[] MINUS_BYTES;
	public static final byte[] POSITIVE_INFINITY_BYTES;
	public static final byte[] NEGATIVE_INFINITY_BYTES;

	private static final long INDEXED_RANGE_START = 0;
	private static final long INDEXED_RANGE_END = -1;

	static {
		PLUS_BYTES = toBytes("+");
		MINUS_BYTES = toBytes("-");
		POSITIVE_INFINITY_BYTES = toBytes("+inf");
		NEGATIVE_INFINITY_BYTES = toBytes("-inf");
	}

	public static Point geoCoordinatesToPoint(@Nullable GeoCoordinates geoCoordinate) {

		return geoCoordinate != null
				? new Point(geoCoordinate.getX().doubleValue(), geoCoordinate.getY().doubleValue())
				: null;
	}

	public static Converter<String, List<ValkeyClientInfo>> stringToValkeyClientListConverter() {
		return LettuceConverters::toListOfValkeyClientInformation;
	}

	public static Converter<List<ScoredValue<byte[]>>, List<Tuple>> scoredValuesToTupleList() {

		return source -> {

			if (source == null) {
				return null;
			}

			List<Tuple> tuples = new ArrayList<>(source.size());

			for (ScoredValue<byte[]> value : source) {
				tuples.add(LettuceConverters.toTuple(value));
			}

			return tuples;
		};
	}

	public static boolean toBoolean(long value) {
		return value == 1;
	}

	/**
	 * @since 1.3
	 */
	public static Converter<Long, Boolean> longToBooleanConverter() {
		return Converters::toBoolean;
	}

	public static Long toLong(@Nullable Date source) {
		return source != null ? source.getTime() : null;
	}

	public static Set<byte[]> toBytesSet(@Nullable List<byte[]> source) {
		return source != null ? new LinkedHashSet<>(source) : null;
	}

	public static List<byte[]> toBytesList(KeyValue<byte[], byte[]> source) {

		if (source == null) {
			return null;
		}

		List<byte[]> list = new ArrayList<>(2);

		list.add(source.getKey());
		list.add(source.getValue());

		return list;
	}

	public static List<byte[]> toBytesList(Collection<byte[]> source) {

		if (source instanceof List) {
			return (List<byte[]>) source;
		}

		return source != null ? new ArrayList<>(source) : null;
	}

	public static Tuple toTuple(@Nullable ScoredValue<byte[]> source) {

		return source != null && source.hasValue()
				? new DefaultTuple(source.getValue(), Double.valueOf(source.getScore()))
				: null;
	}

	public static String toString(@Nullable byte[] source) {

		if (source == null || Arrays.equals(source, new byte[0])) {
			return null;
		}

		return new String(source);
	}

	public static ScriptOutputType toScriptOutputType(ReturnType returnType) {

		Assert.notNull(returnType, () -> "Return type " + returnType + " is not a supported script output type");

		return switch (returnType) {
			case BOOLEAN -> ScriptOutputType.BOOLEAN;
			case MULTI -> ScriptOutputType.MULTI;
			case VALUE -> ScriptOutputType.VALUE;
			case INTEGER -> ScriptOutputType.INTEGER;
			case STATUS -> ScriptOutputType.STATUS;
		};
	}

	public static boolean toBoolean(Position where) {
		Assert.notNull(where, "list positions are mandatory");
		return !Position.AFTER.equals(where);
	}

	public static int toInt(boolean value) {
		return value ? 1 : 0;
	}

	public static Map<byte[], byte[]> toMap(List<byte[]> source) {

		if (CollectionUtils.isEmpty(source)) {
			return Collections.emptyMap();
		}

		Map<byte[], byte[]> target = new LinkedHashMap<>();
		Iterator<byte[]> keyValue = source.iterator();

		while (keyValue.hasNext()) {
			target.put(keyValue.next(), keyValue.hasNext() ? keyValue.next() : null);
		}

		return target;
	}

	public static SortArgs toSortArgs(SortParameters params) {

		SortArgs args = new SortArgs();

		if (params == null) {
			return args;
		}
		if (params.getByPattern() != null) {
			args.by(new String(params.getByPattern(), StandardCharsets.US_ASCII));
		}
		if (params.getLimit() != null) {
			args.limit(params.getLimit().getStart(), params.getLimit().getCount());
		}
		if (params.getGetPattern() != null) {
			byte[][] pattern = params.getGetPattern();
			for (byte[] bs : pattern) {
				args.get(new String(bs, StandardCharsets.US_ASCII));
			}
		}
		if (params.getOrder() != null) {
			if (params.getOrder() == Order.ASC) {
				args.asc();
			} else {
				args.desc();
			}
		}

		Boolean isAlpha = params.isAlphabetic();

		if (isAlpha != null && isAlpha) {
			args.alpha();
		}

		return args;
	}

	public static List<ValkeyClientInfo> toListOfValkeyClientInformation(String clientList) {

		if (!StringUtils.hasText(clientList)) {
			return Collections.emptyList();
		}

		return StringToValkeyClientInfoConverter.INSTANCE.convert(clientList.split("\\r?\\n"));
	}

	/**
	 * Convert a {@link Limit} to a Lettuce {@link io.lettuce.core.Limit}.
	 *
	 * @return a lettuce {@link io.lettuce.core.Limit}.
	 * @since 2.0
	 */
	public static io.lettuce.core.Limit toLimit(Limit limit) {
		return limit.isUnlimited() ? io.lettuce.core.Limit.unlimited()
				: io.lettuce.core.Limit.create(limit.getOffset(), limit.getCount());
	}

	/**
	 * Convert a {@link org.springframework.data.valkey.connection.ValkeyZSetCommands.Range} to a lettuce {@link Range}.
	 *
	 * @since 2.0
	 */
	public static <T> Range<T> toRange(org.springframework.data.domain.Range<T> range) {
		return toRange(range, false);
	}

	/**
	 * Convert a {@link org.springframework.data.domain.Range} to a lettuce {@link Range}.
	 *
	 * @since 2.2
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> Range<T> toRange(org.springframework.data.domain.Range<T> range, boolean convertNumberToBytes) {

		Range.Boundary upper = RangeConverter.convertBound(range.getUpperBound(), convertNumberToBytes, null,
				it -> it.getBytes(StandardCharsets.UTF_8));

		Range.Boundary lower = RangeConverter.convertBound(range.getLowerBound(), convertNumberToBytes, null,
				it -> it.getBytes(StandardCharsets.UTF_8));

		return Range.from(lower, upper);
	}

	/**
	 * Convert a {@link org.springframework.data.domain.Range} to a lettuce {@link Range} and reverse boundaries.
	 *
	 * @since 2.0
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> Range<T> toRevRange(org.springframework.data.domain.Range<T> range) {

		Range.Boundary upper = RangeConverter.convertBound(range.getUpperBound(), false, null,
				it -> it.getBytes(StandardCharsets.UTF_8));

		Range.Boundary lower = RangeConverter.convertBound(range.getLowerBound(), false, null,
				it -> it.getBytes(StandardCharsets.UTF_8));

		return Range.from(upper, lower);
	}

	/**
	 * @param source List of Maps containing node details from SENTINEL REPLICAS or SENTINEL MASTERS. May be empty or
	 *          {@literal null}.
	 * @return List of {@link ValkeyServer}'s. List is empty if List of Maps is empty.
	 * @since 1.5
	 */
	public static List<ValkeyServer> toListOfValkeyServer(List<Map<String, String>> source) {

		if (CollectionUtils.isEmpty(source)) {
			return Collections.emptyList();
		}

		List<ValkeyServer> sentinels = new ArrayList<>();

		for (Map<String, String> info : source) {
			sentinels.add(ValkeyServer.newServerFrom(Converters.toProperties(info)));
		}

		return sentinels;
	}

	/**
	 * @param sentinelConfiguration the sentinel configuration containing one or more sentinels and a master name. Must
	 *          not be {@literal null}
	 * @return A {@link ValkeyURI} containing Valkey Sentinel addresses of {@link ValkeySentinelConfiguration}
	 * @since 1.5
	 */
	public static ValkeyURI sentinelConfigurationToValkeyURI(ValkeySentinelConfiguration sentinelConfiguration) {

		Assert.notNull(sentinelConfiguration, "ValkeySentinelConfiguration is required");

		Set<ValkeyNode> sentinels = sentinelConfiguration.getSentinels();
		ValkeyPassword sentinelPassword = sentinelConfiguration.getSentinelPassword();
		ValkeyURI.Builder builder = ValkeyURI.builder();

		for (ValkeyNode sentinel : sentinels) {

			ValkeyURI.Builder sentinelBuilder = ValkeyURI.Builder.redis(sentinel.getHost(), sentinel.getPort());

			String sentinelUsername = sentinelConfiguration.getSentinelUsername();
			if (StringUtils.hasText(sentinelUsername) && sentinelPassword.isPresent()) {
				// See https://github.com/lettuce-io/lettuce-core/issues/1404
				sentinelBuilder.withAuthentication(sentinelUsername, sentinelPassword.get());
			} else {
				sentinelPassword.toOptional().ifPresent(sentinelBuilder::withPassword);
			}

			builder.withSentinel(sentinelBuilder.build());
		}

		String username = sentinelConfiguration.getUsername();
		ValkeyPassword password = sentinelConfiguration.getPassword();

		if (StringUtils.hasText(username) && password.isPresent()) {
			// See https://github.com/lettuce-io/lettuce-core/issues/1404
			builder.withAuthentication(username, password.get());
		} else {
			password.toOptional().ifPresent(builder::withPassword);
		}

		builder.withSentinelMasterId(sentinelConfiguration.getMaster().getName());

		return builder.build();
	}

	/**
	 * Converts a {@link ValkeyURI} to its corresponding {@link ValkeyStandaloneConfiguration}.
	 *
	 * @param redisURI the uri containing the Valkey connection info
	 * @return a {@link ValkeyStandaloneConfiguration} representing the connection information in the Valkey URI.
	 * @since 2.5.3
	 */
	static ValkeyStandaloneConfiguration createValkeyStandaloneConfiguration(ValkeyURI redisURI) {

		ValkeyStandaloneConfiguration standaloneConfiguration = new ValkeyStandaloneConfiguration();

		standaloneConfiguration.setHostName(redisURI.getHost());
		standaloneConfiguration.setPort(redisURI.getPort());
		standaloneConfiguration.setDatabase(redisURI.getDatabase());

		applyAuthentication(redisURI, standaloneConfiguration);

		return standaloneConfiguration;
	}

	/**
	 * Converts a {@link ValkeyURI} to its corresponding {@link ValkeySocketConfiguration}.
	 *
	 * @param redisURI the uri containing the Valkey connection info using a local unix domain socket
	 * @return a {@link ValkeySocketConfiguration} representing the connection information in the Valkey URI.
	 * @since 2.5.3
	 */
	static ValkeySocketConfiguration createValkeySocketConfiguration(ValkeyURI redisURI) {

		ValkeySocketConfiguration socketConfiguration = new ValkeySocketConfiguration();

		socketConfiguration.setSocket(redisURI.getSocket());
		socketConfiguration.setDatabase(redisURI.getDatabase());

		applyAuthentication(redisURI, socketConfiguration);

		return socketConfiguration;
	}

	/**
	 * Converts a {@link ValkeyURI} to its corresponding {@link ValkeySentinelConfiguration}.
	 *
	 * @param redisURI the uri containing the Valkey Sentinel connection info
	 * @return a {@link ValkeySentinelConfiguration} representing the Valkey Sentinel information in the Valkey URI.
	 * @since 2.5.3
	 */
	static ValkeySentinelConfiguration createValkeySentinelConfiguration(ValkeyURI redisURI) {

		ValkeySentinelConfiguration sentinelConfiguration = new ValkeySentinelConfiguration();

		if (!ObjectUtils.isEmpty(redisURI.getSentinelMasterId())) {
			sentinelConfiguration.setMaster(redisURI.getSentinelMasterId());
		}

		sentinelConfiguration.setDatabase(redisURI.getDatabase());

		for (ValkeyURI sentinelNodeValkeyUri : redisURI.getSentinels()) {

			ValkeyNode sentinelNode = new ValkeyNode(sentinelNodeValkeyUri.getHost(), sentinelNodeValkeyUri.getPort());

			if (sentinelNodeValkeyUri.getPassword() != null) {
				sentinelConfiguration.setSentinelPassword(sentinelNodeValkeyUri.getPassword());
			}

			sentinelConfiguration.addSentinel(sentinelNode);
		}

		applyAuthentication(redisURI, sentinelConfiguration);

		return sentinelConfiguration;
	}

	private static void applyAuthentication(ValkeyURI redisURI, ValkeyConfiguration.WithAuthentication redisConfiguration) {

		if (StringUtils.hasText(redisURI.getUsername())) {
			redisConfiguration.setUsername(redisURI.getUsername());
		}

		if (redisURI.getPassword() != null) {
			redisConfiguration.setPassword(redisURI.getPassword());
		}
	}

	public static byte[] toBytes(@Nullable String source) {
		return source != null ? source.getBytes() : null;
	}

	public static byte[] toBytes(Integer source) {
		return String.valueOf(source).getBytes();
	}

	public static byte[] toBytes(Long source) {
		return String.valueOf(source).getBytes();
	}

	/**
	 * @since 1.6
	 */
	public static byte[] toBytes(Double source) {
		return toBytes(String.valueOf(source));
	}

	public static List<ValkeyClusterNode> partitionsToClusterNodes(@Nullable Partitions source) {

		if (source == null) {
			return Collections.emptyList();
		}

		List<ValkeyClusterNode> nodes = new ArrayList<>();

		for (io.lettuce.core.cluster.models.partitions.ValkeyClusterNode node : source) {
			nodes.add(toValkeyClusterNode(node));
		}

		return nodes;
	}

	/**
	 * @since 1.7
	 */
	public static ValkeyClusterNode toValkeyClusterNode(io.lettuce.core.cluster.models.partitions.ValkeyClusterNode source) {

		Set<Flag> flags = parseFlags(source.getFlags());

		return ValkeyClusterNode.newValkeyClusterNode().listeningAt(source.getUri().getHost(), source.getUri().getPort())
				.withId(source.getNodeId()).promotedAs(flags.contains(Flag.MASTER) ? NodeType.MASTER : NodeType.REPLICA)
				.serving(new SlotRange(source.getSlots())).withFlags(flags)
				.linkState(source.isConnected() ? LinkState.CONNECTED : LinkState.DISCONNECTED).replicaOf(source.getSlaveOf())
				.build();
	}

	private static Set<Flag> parseFlags(@Nullable Set<NodeFlag> source) {

		Set<Flag> flags = new LinkedHashSet<>(source != null ? source.size() : 8, 1);

		for (NodeFlag flag : source) {
			if (Objects.nonNull(flag)) {
				switch (flag) {
					case NOFLAGS -> flags.add(Flag.NOFLAGS);
					case EVENTUAL_FAIL -> flags.add(Flag.PFAIL);
					case FAIL -> flags.add(Flag.FAIL);
					case HANDSHAKE -> flags.add(Flag.HANDSHAKE);
					case MASTER -> flags.add(Flag.MASTER);
					case MYSELF -> flags.add(Flag.MYSELF);
					case NOADDR -> flags.add(Flag.NOADDR);
					case SLAVE, REPLICA -> flags.add(Flag.REPLICA);
				}
			}
		}

		return flags;
	}

	/**
	 * Converts a given {@link Expiration} and {@link SetOption} to the according {@link SetArgs}.<br />
	 *
	 * @param expiration can be {@literal null}.
	 * @param option can be {@literal null}.
	 * @since 1.7
	 */
	public static SetArgs toSetArgs(@Nullable Expiration expiration, @Nullable SetOption option) {

		SetArgs args = new SetArgs();

		if (expiration != null) {

			if (expiration.isKeepTtl()) {
				args.keepttl();
			} else if (!expiration.isPersistent()) {

				switch (expiration.getTimeUnit()) {
					case MILLISECONDS -> {
						if (expiration.isUnixTimestamp()) {
							args.pxAt(expiration.getConverted(TimeUnit.MILLISECONDS));
						} else {
							args.px(expiration.getConverted(TimeUnit.MILLISECONDS));
						}
					}
					default -> {
						if (expiration.isUnixTimestamp()) {
							args.exAt(expiration.getConverted(TimeUnit.SECONDS));
						} else {
							args.ex(expiration.getConverted(TimeUnit.SECONDS));
						}
					}
				}
			}
		}

		if (option != null) {
			switch (option) {
				case SET_IF_ABSENT -> args.nx();
				case SET_IF_PRESENT -> args.xx();
			}
		}

		return args;
	}

	/**
	 * Convert {@link Expiration} to {@link GetExArgs}.
	 *
	 * @param expiration can be {@literal null}.
	 * @since 2.6
	 */
	static GetExArgs toGetExArgs(@Nullable Expiration expiration) {

		GetExArgs args = new GetExArgs();

		if (expiration == null) {
			return args;
		}

		if (expiration.isPersistent()) {
			return args.persist();
		}

		if (expiration.getTimeUnit() == TimeUnit.MILLISECONDS) {
			if (expiration.isUnixTimestamp()) {
				return args.pxAt(expiration.getExpirationTime());
			}
			return args.px(expiration.getExpirationTime());
		}

		return expiration.isUnixTimestamp() ? args.exAt(expiration.getConverted(TimeUnit.SECONDS))
				: args.ex(expiration.getConverted(TimeUnit.SECONDS));
	}

	static Converter<List<byte[]>, Long> toTimeConverter(TimeUnit timeUnit) {

		return source -> {

			Assert.notEmpty(source, "Received invalid result from server; Expected 2 items in collection");
			Assert.isTrue(source.size() == 2,
					"Received invalid nr of arguments from redis server; Expected 2 received " + source.size());

			return toTimeMillis(toString(source.get(0)), toString(source.get(1)), timeUnit);
		};
	}

	/**
	 * Convert {@link Metric} into {@link GeoArgs.Unit}.
	 *
	 * @since 1.8
	 */
	public static GeoArgs.Unit toGeoArgsUnit(Metric metric) {

		Metric metricToUse = metric == null
				|| ObjectUtils.nullSafeEquals(Metrics.NEUTRAL, metric) ? DistanceUnit.METERS : metric;

		return ObjectUtils.caseInsensitiveValueOf(GeoArgs.Unit.values(), metricToUse.getAbbreviation());
	}

	/**
	 * Convert {@link GeoRadiusCommandArgs} into {@link GeoArgs}.
	 *
	 * @since 1.8
	 */
	public static GeoArgs toGeoArgs(GeoRadiusCommandArgs args) {
		return toGeoArgs((GeoCommandArgs) args);
	}

	/**
	 * Convert {@link GeoCommandArgs} into {@link GeoArgs}.
	 *
	 * @since 2.6
	 */
	public static GeoArgs toGeoArgs(GeoCommandArgs args) {

		GeoArgs geoArgs = new GeoArgs();

		if (args.hasFlags()) {
			for (GeoCommandArgs.GeoCommandFlag flag : args.getFlags()) {
				if (flag.equals(GeoRadiusCommandArgs.Flag.WITHCOORD)) {
					geoArgs.withCoordinates();
				} else if (flag.equals(GeoRadiusCommandArgs.Flag.WITHDIST)) {
					geoArgs.withDistance();
				}
			}
		}

		if (args.hasSortDirection()) {
			switch (args.getSortDirection()) {
				case ASC -> geoArgs.asc();
				case DESC -> geoArgs.desc();
			}
		}

		if (args.hasLimit()) {
			geoArgs.withCount(args.getLimit(), args.getFlags().contains(GeoRadiusCommandArgs.Flag.ANY));
		}

		return geoArgs;
	}

	/**
	 * Convert {@link BitFieldSubCommands} into {@link BitFieldArgs}.
	 *
	 * @since 2.1
	 */
	public static BitFieldArgs toBitFieldArgs(BitFieldSubCommands subCommands) {

		BitFieldArgs args = new BitFieldArgs();

		for (BitFieldSubCommand subCommand : subCommands) {

			BitFieldArgs.BitFieldType bitFieldType = subCommand.getType().isSigned()
					? BitFieldArgs.signed(subCommand.getType().getBits())
					: BitFieldArgs.unsigned(subCommand.getType().getBits());

			BitFieldArgs.Offset offset = subCommand.getOffset().isZeroBased()
					? BitFieldArgs.offset((int) subCommand.getOffset().getValue())
					: BitFieldArgs.typeWidthBasedOffset((int) subCommand.getOffset().getValue());

			if (subCommand instanceof BitFieldGet) {
				args = args.get(bitFieldType, offset);
			} else if (subCommand instanceof BitFieldSet) {
				args = args.set(bitFieldType, offset, ((BitFieldSet) subCommand).getValue());
			} else if (subCommand instanceof BitFieldIncrBy) {

				BitFieldIncrBy.Overflow overflow = ((BitFieldIncrBy) subCommand).getOverflow();

				if (overflow != null) {

					BitFieldArgs.OverflowType type = switch (overflow) {
						case SAT -> BitFieldArgs.OverflowType.SAT;
						case FAIL -> BitFieldArgs.OverflowType.FAIL;
						case WRAP -> BitFieldArgs.OverflowType.WRAP;
          			};

					args = args.overflow(type);
				}

				args = args.incrBy(bitFieldType, offset, ((BitFieldIncrBy) subCommand).getValue());
			}
		}

		return args;
	}

	/**
	 * Convert {@link ScanOptions} to {@link ScanArgs}.
	 *
	 * @param options the {@link ScanOptions} to convert, may be {@literal null}.
	 * @return the converted {@link ScanArgs}. Returns {@literal null} if {@link ScanOptions} is {@literal null}.
	 * @see 2.1
	 */
	@Nullable
	static ScanArgs toScanArgs(@Nullable ScanOptions options) {

		if (options == null) {
			return null;
		}

		KeyScanArgs scanArgs = new KeyScanArgs();

		byte[] pattern = options.getBytePattern();

		if (pattern != null) {
			scanArgs.match(pattern);
		}

		if (options.getCount() != null) {
			scanArgs.limit(options.getCount());
		}

		if (options instanceof KeyScanOptions) {
			scanArgs.type(((KeyScanOptions) options).getType());
		}

		return scanArgs;
	}

	/**
	 * Get {@link Converter} capable of {@link Set} of {@link Byte} into {@link GeoResults}.
	 *
	 * @since 1.8
	 */
	public static Converter<Set<byte[]>, GeoResults<GeoLocation<byte[]>>> bytesSetToGeoResultsConverter() {

		return source -> {

			if (CollectionUtils.isEmpty(source)) {
				return new GeoResults<>(Collections.<GeoResult<GeoLocation<byte[]>>> emptyList());
			}

			List<GeoResult<GeoLocation<byte[]>>> results = new ArrayList<>(source.size());
			Iterator<byte[]> it = source.iterator();

			while (it.hasNext()) {
				results.add(new GeoResult<>(new GeoLocation<>(it.next(), null), new Distance(0D)));
			}

			return new GeoResults<>(results);
		};
	}

	/**
	 * Get {@link Converter} capable of convering {@link GeoWithin} into {@link GeoResults}.
	 *
	 * @since 1.8
	 */
	public static Converter<List<GeoWithin<byte[]>>, GeoResults<GeoLocation<byte[]>>> geoRadiusResponseToGeoResultsConverter(
			Metric metric) {
		return GeoResultsConverterFactory.INSTANCE.forMetric(metric);
	}

	public static Converter<TransactionResult, List<Object>> transactionResultUnwrapper() {
		return transactionResult -> transactionResult.stream().collect(Collectors.toList());
	}

	/**
	 * Return {@link Optional} lower bound from {@link Range}.
	 *
	 * @since 2.0.9
	 */
	static <T extends Comparable<T>> Optional<T> getLowerBound(org.springframework.data.domain.Range<T> range) {
		return range.getLowerBound().getValue();
	}

	/**
	 * Return {@link Optional} upper bound from {@link Range}.
	 *
	 * @since 2.0.9
	 */
	static <T extends Comparable<T>> Optional<T> getUpperBound(org.springframework.data.domain.Range<T> range) {
		return range.getUpperBound().getValue();
	}

	/**
	 * Return the lower bound index from {@link Range} or {@literal 0} (zero) if the lower range is not bounded to point
	 * to the first element. To be used with index-based commands such as {@code LRANGE}, {@code GETRANGE}.
	 *
	 * @return the lower index bound value or {@literal 0} for the first element if not bounded.
	 * @since 2.0.9
	 */
	static long getLowerBoundIndex(org.springframework.data.domain.Range<Long> range) {
		return getLowerBound(range).orElse(INDEXED_RANGE_START);
	}

	/**
	 * Return the upper bound index from {@link Range} or {@literal -1} (minus one) if the upper range is not bounded to
	 * point to the last element. To be used with index-based commands such as {@code LRANGE}, {@code GETRANGE}.
	 *
	 * @return the upper index bound value or {@literal -1} for the last element if not bounded.
	 * @since 2.0.9
	 */
	static long getUpperBoundIndex(org.springframework.data.domain.Range<Long> range) {
		return getUpperBound(range).orElse(INDEXED_RANGE_END);
	}

	static LMoveArgs toLmoveArgs(Enum<?> from, Enum<?> to) {

		if (from.name().equals(Direction.LEFT.name())) {
			if (to.name().equals(Direction.LEFT.name())) {
				return LMoveArgs.Builder.leftLeft();
			}
			return LMoveArgs.Builder.leftRight();
		}

		if (to.name().equals(Direction.LEFT.name())) {
			return LMoveArgs.Builder.rightLeft();
		}
		return LMoveArgs.Builder.rightRight();
	}

	static GeoSearch.GeoPredicate toGeoPredicate(GeoShape predicate) {

		if (predicate instanceof RadiusShape) {

			Distance radius = ((RadiusShape) predicate).getRadius();

			return GeoSearch.byRadius(radius.getValue(), toGeoArgsUnit(radius.getMetric()));
		}

		if (predicate instanceof BoxShape boxPredicate) {

			BoundingBox boundingBox = boxPredicate.getBoundingBox();

			return GeoSearch.byBox(boundingBox.getWidth().getValue(), boundingBox.getHeight().getValue(),
					toGeoArgsUnit(boxPredicate.getMetric()));
		}

		throw new IllegalArgumentException("Cannot convert %s to Lettuce GeoPredicate".formatted(predicate));
	}

	static <T> GeoSearch.GeoRef<T> toGeoRef(GeoReference<T> reference) {

		if (reference instanceof GeoReference.GeoMemberReference) {
			return GeoSearch.fromMember(((GeoMemberReference<T>) reference).getMember());
		}

		if (reference instanceof GeoCoordinateReference<?> coordinates) {

			return GeoSearch.fromCoordinates(coordinates.getLongitude(), coordinates.getLatitude());
		}

		throw new IllegalArgumentException("Cannot convert %s to Lettuce GeoRef".formatted(reference));
	}

	static FlushMode toFlushMode(@Nullable ValkeyServerCommands.FlushOption option) {

		if (option == null) {
			return FlushMode.SYNC;
		}

		return switch (option) {
			case ASYNC -> FlushMode.ASYNC;
			case SYNC -> FlushMode.SYNC;
    	};
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	enum GeoResultsConverterFactory {

		INSTANCE;

		Converter<List<GeoWithin<byte[]>>, GeoResults<GeoLocation<byte[]>>> forMetric(Metric metric) {
			return new GeoResultsConverter(metric == null
					|| ObjectUtils.nullSafeEquals(Metrics.NEUTRAL, metric) ? DistanceUnit.METERS : metric);
		}

		private static class GeoResultsConverter
				implements Converter<List<GeoWithin<byte[]>>, GeoResults<GeoLocation<byte[]>>> {

			private final Metric metric;

			public GeoResultsConverter(Metric metric) {
				this.metric = metric;
			}

			@Override
			public GeoResults<GeoLocation<byte[]>> convert(List<GeoWithin<byte[]>> source) {

				List<GeoResult<GeoLocation<byte[]>>> results = new ArrayList<>(source.size());

				Converter<GeoWithin<byte[]>, GeoResult<GeoLocation<byte[]>>> converter = GeoResultConverterFactory.INSTANCE
						.forMetric(metric);
				for (GeoWithin<byte[]> result : source) {
					results.add(converter.convert(result));
				}

				return new GeoResults<>(results, metric);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	enum GeoResultConverterFactory {

		INSTANCE;

		Converter<GeoWithin<byte[]>, GeoResult<GeoLocation<byte[]>>> forMetric(Metric metric) {
			return new GeoResultConverter(metric);
		}

		private static class GeoResultConverter implements Converter<GeoWithin<byte[]>, GeoResult<GeoLocation<byte[]>>> {

			private final Metric metric;

			public GeoResultConverter(Metric metric) {
				this.metric = metric;
			}

			@Override
			public GeoResult<GeoLocation<byte[]>> convert(GeoWithin<byte[]> source) {

				Point point = geoCoordinatesToPoint(source.getCoordinates());

				return new GeoResult<>(new GeoLocation<>(source.getMember(), point),
						new Distance(source.getDistance() != null ? source.getDistance() : 0D, metric));
			}
		}
	}
}