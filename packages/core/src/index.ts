function isPromise(input: any): input is Promise<any> {
  return input instanceof Promise;
}

export type Mappable<T> = {
  toEntity: (raw: any) => Promise<T>;
};

export type MappableInputType<T extends Mappable<any>> = Parameters<T["toEntity"]>[0];
export type MappableOutputType<T extends Mappable<any>> = Awaited<ReturnType<T["toEntity"]>>;

export type Relationship<
  LocalEntityDef extends EntityDefinition<any>,
  OtherEntityDef extends EntityDefinition<any>
> = {
  otherEntity: OtherEntityDef;
  load: (entities: Array<MappableOutputType<LocalEntityDef>>) => AsyncIterable<MappableInputType<OtherEntityDef>> | Promise<Iterable<MappableInputType<OtherEntityDef>>>;
  attach: (otherEntities: Array<MappableOutputType<OtherEntityDef>>) => (entity: MappableOutputType<LocalEntityDef>) => MappableOutputType<OtherEntityDef> | Array<MappableOutputType<OtherEntityDef>> | null;
};

export type EntityDefinition<T> = Mappable<T>;

type RelationsToLoad = {
  [key: string]: Relationship<EntityDefinition<any>, EntityDefinition<any>> | [Relationship<EntityDefinition<any>, EntityDefinition<any>>, RelationsToLoad];
};

type WithLoadedRelations<Entity, Relations extends RelationsToLoad> = Entity & {
  [K in keyof Relations]: Relations[K] extends [Relationship<any, any>, RelationsToLoad]
      // [relationship, nested relations]
      ? (
          ReturnType<ReturnType<Relations[K][0]["attach"]>> extends any[]
              // Relationship returns an array
              ? Array<ReturnType<ReturnType<Relations[K][0]["attach"]>>[number] & WithLoadedRelations<ReturnType<ReturnType<Relations[K][0]["attach"]>>[number], Relations[K][1]>>
              // Relationship returns an entity or null
              : (ReturnType<ReturnType<Relations[K][0]["attach"]>> & WithLoadedRelations<ReturnType<ReturnType<Relations[K][0]["attach"]>>, Relations[K][1]>) | null
          )
      // Just relationship
      : Relations[K] extends Relationship<any, any>
          ? ReturnType<ReturnType<Relations[K]["attach"]>>
          : never;
};

export function mapToEntity<EntityDef extends EntityDefinition<any>>(entityDef: EntityDef, data: MappableInputType<EntityDef>) {
  return entityDef.toEntity(data);
}

export async function mapArrayToEntity<EntityDef extends EntityDefinition<any>>(entityDef: EntityDef, arr: AsyncIterable<MappableInputType<EntityDef>> | Iterable<MappableInputType<EntityDef>>) {
  const result: Array<MappableOutputType<EntityDef>> = [];
  for await (const data of arr) {
      result.push(await entityDef.toEntity(data));
  }
  return result;
}

export async function loadRelationsForArray<Entity, Relations extends RelationsToLoad>(
  entities: Entity[],
  relations: Relations
): Promise<Array<WithLoadedRelations<Entity, Relations>>> {
  await Promise.all(Object.entries(relations).map(async ([key, relationshipOrNested]) => {
      const relationship = Array.isArray(relationshipOrNested) ? relationshipOrNested[0] : relationshipOrNested;
      const subrelations = Array.isArray(relationshipOrNested) ? relationshipOrNested[1] : {};

      const loadedEntities: any[] = [];
      const rawIterable = relationship.load(entities);
      const iterable = isPromise(rawIterable) ? await rawIterable : rawIterable;
      for await (const data of iterable) {
          loadedEntities.push(await relationship.otherEntity.toEntity(data));
      }
      const loadedEntitiesWithNestedRelations = Object.keys(subrelations).length
          ? await loadRelationsForArray(loadedEntities, subrelations)
          : loadedEntities;
      const attachFn = relationship.attach(loadedEntitiesWithNestedRelations);
      for (const entity of entities) {
          (entity as WithLoadedRelations<Entity, Relations>)[key as keyof WithLoadedRelations<Entity, Relations>] = attachFn(entity);
      }
  }));

  return entities as Array<WithLoadedRelations<Entity, Relations>>;
}

export async function loadRelationsForEntity<Entity, Relations extends RelationsToLoad>(
  entity: Entity,
  relations: Relations
): Promise<WithLoadedRelations<Entity, Relations>> {
  return (await loadRelationsForArray([entity], relations))[0];
}
